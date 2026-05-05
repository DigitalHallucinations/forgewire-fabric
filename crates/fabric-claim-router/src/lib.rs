//! Capability-aware claim routing for ForgeWire.
//!
//! Mirrors the Python `Blackboard.claim_next_task_v2` per-task match loop in
//! `scripts/remote/hub/server.py`. The pre-checks (drain, concurrency cap,
//! resource gates) stay in Python because they are DB-bound and cheap; the
//! O(N) candidate filter is the hot path.
//!
//! The match contract per candidate task, in order:
//!
//! 1. **Tenant gate**: if the task pins a tenant, the runner's tenant must equal it.
//! 2. **Workspace gate**: if both the task and runner pin a workspace_root, they must equal.
//! 3. **Scope prefix affinity**: every task glob's leading static prefix must
//!    overlap (either direction) with at least one runner prefix. Empty
//!    runner prefixes mean "accept everything".
//! 4. **Required tools**: every task tool (lowercased) must be in the runner's tool set.
//! 5. **Required tags**: every task tag (lowercased) must be in the runner's tag set.
//! 6. **Base-commit precondition**: if the task has `require_base_commit`,
//!    the runner's `last_known_commit` must equal the task's `base_commit`.
//!
//! `pick_task` returns the first candidate index that passes all six checks,
//! or `None` if every candidate is rejected. It does not own the SQLite
//! transaction — the caller does.

#[derive(Debug, Clone)]
pub struct CandidateTask {
    pub scope_globs: Vec<String>,
    pub required_tools: Vec<String>,
    pub required_tags: Vec<String>,
    pub tenant: Option<String>,
    pub workspace_root: Option<String>,
    pub require_base_commit: bool,
    pub base_commit: String,
}

#[derive(Debug, Clone)]
pub struct RunnerView {
    /// Pre-normalized: backslashes → slashes, trailing `/`, empties stripped.
    pub scope_prefixes: Vec<String>,
    /// Original case preserved; matching uses `eq_ignore_ascii_case` so neither
    /// side has to allocate a lowercased copy on the hot path.
    pub tools: Vec<String>,
    pub tags: Vec<String>,
    pub tenant: Option<String>,
    pub workspace_root: Option<String>,
    pub last_known_commit: Option<String>,
}

impl RunnerView {
    pub fn from_raw(
        scope_prefixes: &[String],
        tools: &[String],
        tags: &[String],
        tenant: Option<String>,
        workspace_root: Option<String>,
        last_known_commit: Option<String>,
    ) -> Self {
        let scope_prefixes_norm = scope_prefixes
            .iter()
            .filter(|p| !p.is_empty())
            .map(|p| {
                let mut s = p.replace('\\', "/");
                while s.ends_with('/') {
                    s.pop();
                }
                s.push('/');
                s
            })
            .collect();
        Self {
            scope_prefixes: scope_prefixes_norm,
            tools: tools.to_vec(),
            tags: tags.to_vec(),
            tenant,
            workspace_root,
            last_known_commit,
        }
    }
}

/// Return the leading wildcard-free directory prefix of a glob, as a borrow
/// into the caller's input plus a synthetic trailing `/` indicator.
///
/// e.g. `modules/jobs/**` → `modules/jobs/`, `tests/**/test_x.py` → `tests/`,
/// `foo` (no slash, no wildcards) → `""`.
///
/// Allocates only when the input contains a backslash that needs normalizing.
pub fn glob_static_prefix(glob: &str) -> String {
    let mut had_backslash = false;
    for b in glob.bytes() {
        if b == b'\\' {
            had_backslash = true;
            break;
        }
    }
    let norm: std::borrow::Cow<'_, str> = if had_backslash {
        std::borrow::Cow::Owned(glob.replace('\\', "/"))
    } else {
        std::borrow::Cow::Borrowed(glob)
    };
    let cut = norm
        .find(|c: char| c == '*' || c == '?' || c == '[')
        .unwrap_or(norm.len());
    let head = &norm[..cut];
    match head.rfind('/') {
        Some(i) => format!("{}/", &head[..i]),
        None => String::new(),
    }
}

/// True iff every task glob's static prefix overlaps with some runner prefix.
pub fn scopes_within(task_globs: &[String], runner_prefixes: &[String]) -> bool {
    if runner_prefixes.is_empty() {
        return true;
    }
    for glob in task_globs {
        let head = glob_static_prefix(glob);
        let ok = runner_prefixes
            .iter()
            .any(|p| head.starts_with(p) || p.starts_with(&head));
        if !ok {
            return false;
        }
    }
    true
}

/// Same as [`scopes_within`] but operates on borrowed `&str` slices, avoiding
/// `Vec<String>` allocation on the hot PyO3 path.
pub fn scopes_within_strs(globs: &[&str], runner_prefixes: &[String]) -> bool {
    if runner_prefixes.is_empty() {
        return true;
    }
    for glob in globs {
        let head = glob_static_prefix(glob);
        let ok = runner_prefixes
            .iter()
            .any(|p| head.starts_with(p) || p.starts_with(&head));
        if !ok {
            return false;
        }
    }
    true
}

/// True iff a single candidate satisfies the runner's match contract.
///
/// Public so PyO3 / other callers can drive the loop with lazy extraction.
pub fn matches(task: &CandidateTask, runner: &RunnerView) -> bool {
    if let Some(t) = &task.tenant {
        if runner.tenant.as_deref() != Some(t.as_str()) {
            return false;
        }
    }
    if let (Some(t_ws), Some(r_ws)) = (&task.workspace_root, &runner.workspace_root) {
        if t_ws != r_ws {
            return false;
        }
    }
    if !scopes_within(&task.scope_globs, &runner.scope_prefixes) {
        return false;
    }
    if !task.required_tools.iter().all(|t| {
        runner
            .tools
            .iter()
            .any(|rt| rt.eq_ignore_ascii_case(t))
    }) {
        return false;
    }
    if !task.required_tags.iter().all(|t| {
        runner.tags.iter().any(|rt| rt.eq_ignore_ascii_case(t))
    }) {
        return false;
    }
    if task.require_base_commit {
        match &runner.last_known_commit {
            None => return false,
            Some(rc) if rc != &task.base_commit => return false,
            _ => {}
        }
    }
    true
}

/// Pick the first candidate that passes the full match contract.
///
/// Returns `(Some(idx), candidates_seen)` if a match was found, or
/// `(None, candidates_seen)` if every candidate was rejected.
pub fn pick_task(tasks: &[CandidateTask], runner: &RunnerView) -> (Option<usize>, usize) {
    let mut seen = 0usize;
    for (idx, task) in tasks.iter().enumerate() {
        seen += 1;
        if matches(task, runner) {
            return (Some(idx), seen);
        }
    }
    (None, seen)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(globs: &[&str]) -> CandidateTask {
        CandidateTask {
            scope_globs: globs.iter().map(|s| s.to_string()).collect(),
            required_tools: vec![],
            required_tags: vec![],
            tenant: None,
            workspace_root: None,
            require_base_commit: false,
            base_commit: "deadbeef".into(),
        }
    }

    fn runner(prefixes: &[&str], tools: &[&str], tags: &[&str]) -> RunnerView {
        RunnerView::from_raw(
            &prefixes.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            &tools.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            &tags.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            None,
            None,
            None,
        )
    }

    #[test]
    fn glob_static_prefix_basic() {
        assert_eq!(glob_static_prefix("modules/jobs/**"), "modules/jobs/");
        assert_eq!(glob_static_prefix("tests/**/test_x.py"), "tests/");
        assert_eq!(glob_static_prefix("docs/research/foo.md"), "docs/research/");
        assert_eq!(glob_static_prefix("**/*.py"), "");
        assert_eq!(glob_static_prefix("foo.py"), "");
    }

    #[test]
    fn scopes_within_overlap_either_direction() {
        // Runner prefix is broader than task prefix.
        assert!(scopes_within(
            &vec!["modules/jobs/**".into()],
            &vec!["modules/".into()]
        ));
        // Task prefix is broader than runner prefix.
        assert!(scopes_within(
            &vec!["modules/**".into()],
            &vec!["modules/jobs/".into()]
        ));
        // Disjoint.
        assert!(!scopes_within(
            &vec!["modules/jobs/**".into()],
            &vec!["docs/".into()]
        ));
    }

    #[test]
    fn empty_runner_prefixes_accept_everything() {
        assert!(scopes_within(&vec!["modules/jobs/**".into()], &vec![]));
    }

    #[test]
    fn picks_first_matching_task() {
        let tasks = vec![
            task(&["docs/foo.md"]),    // skipped: scope mismatch
            task(&["modules/jobs/x"]), // claimed
            task(&["docs/bar.md"]),
        ];
        let r = runner(&["modules/"], &[], &[]);
        let (idx, seen) = pick_task(&tasks, &r);
        assert_eq!(idx, Some(1));
        assert_eq!(seen, 2);
    }

    #[test]
    fn required_tag_filters_correctly() {
        let mut t = task(&["docs/foo.md"]);
        t.required_tags = vec!["persona:Researcher".into()];
        let tasks = vec![t];
        let r1 = runner(&["docs/"], &[], &["persona:researcher"]); // case-insensitive
        assert_eq!(pick_task(&tasks, &r1).0, Some(0));
        let r2 = runner(&["docs/"], &[], &["persona:forge"]);
        assert_eq!(pick_task(&tasks, &r2).0, None);
    }

    #[test]
    fn base_commit_precondition() {
        let mut t = task(&["docs/foo.md"]);
        t.require_base_commit = true;
        t.base_commit = "abc1234".into();
        let tasks = vec![t];

        let mut r = runner(&["docs/"], &[], &[]);
        assert_eq!(pick_task(&tasks, &r).0, None); // missing last_known_commit

        r.last_known_commit = Some("zzz9999".into());
        assert_eq!(pick_task(&tasks, &r).0, None); // mismatch

        r.last_known_commit = Some("abc1234".into());
        assert_eq!(pick_task(&tasks, &r).0, Some(0));
    }

    #[test]
    fn tenant_gate() {
        let mut t = task(&["docs/foo.md"]);
        t.tenant = Some("alpha".into());
        let tasks = vec![t];

        let r1 = RunnerView::from_raw(
            &["docs/".into()],
            &[],
            &[],
            Some("alpha".into()),
            None,
            None,
        );
        assert_eq!(pick_task(&tasks, &r1).0, Some(0));

        let r2 = RunnerView::from_raw(
            &["docs/".into()],
            &[],
            &[],
            Some("beta".into()),
            None,
            None,
        );
        assert_eq!(pick_task(&tasks, &r2).0, None);

        let r3 = RunnerView::from_raw(&["docs/".into()], &[], &[], None, None, None);
        assert_eq!(pick_task(&tasks, &r3).0, None);
    }
}
