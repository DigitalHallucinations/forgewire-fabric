//! PyO3 bindings exposing the ForgeWire Rust runtime to Python.
//!
//! Stage C.1 surface:
//! - `verify_signature(public_key_hex: str, payload: bytes, signature_hex: str) -> bool`
//! - `sign_payload(secret_key_hex: str, payload: bytes) -> str`
//! - `canonicalize(envelope: dict) -> bytes`
//! - `verify_envelope(public_key_hex: str, envelope: dict, signature_hex: str) -> bool`
//! - `sign_envelope(secret_key_hex: str, envelope: dict) -> str`
//!
//! Errors map to `ValueError` for shape problems; verification mismatches return
//! `False` rather than raising, matching the Python helper's behavior.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList, PyTuple};
use pythonize::depythonize;
use serde_json::Value;

use fw_claim_router::RunnerView;
use fw_streams::StreamCounter;
use fw_protocol::{
    canonicalize as canonicalize_rs, sign_envelope_hex, sign_payload_hex,
    verify_envelope_hex, verify_signature_hex, ProtocolError,
};

fn map_err(e: ProtocolError) -> PyErr {
    PyValueError::new_err(e.to_string())
}

#[pyfunction]
#[pyo3(signature = (public_key_hex, payload, signature_hex))]
fn verify_signature(public_key_hex: &str, payload: &[u8], signature_hex: &str) -> PyResult<bool> {
    verify_signature_hex(public_key_hex, payload, signature_hex).map_err(map_err)
}

#[pyfunction]
#[pyo3(signature = (secret_key_hex, payload))]
fn sign_payload(secret_key_hex: &str, payload: &[u8]) -> PyResult<String> {
    sign_payload_hex(secret_key_hex, payload).map_err(map_err)
}

#[pyfunction]
fn canonicalize<'py>(py: Python<'py>, envelope: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
    let value: Value = depythonize(envelope)
        .map_err(|e| PyValueError::new_err(format!("envelope must be JSON-compatible: {e}")))?;
    let bytes = canonicalize_rs(&value).map_err(map_err)?;
    Ok(PyBytes::new(py, &bytes))
}

#[pyfunction]
#[pyo3(signature = (public_key_hex, envelope, signature_hex))]
fn verify_envelope(
    public_key_hex: &str,
    envelope: &Bound<'_, PyAny>,
    signature_hex: &str,
) -> PyResult<bool> {
    let value: Value = depythonize(envelope)
        .map_err(|e| PyValueError::new_err(format!("envelope must be JSON-compatible: {e}")))?;
    verify_envelope_hex(public_key_hex, &value, signature_hex).map_err(map_err)
}

#[pyfunction]
#[pyo3(signature = (secret_key_hex, envelope))]
fn sign_envelope(secret_key_hex: &str, envelope: &Bound<'_, PyAny>) -> PyResult<String> {
    let value: Value = depythonize(envelope)
        .map_err(|e| PyValueError::new_err(format!("envelope must be JSON-compatible: {e}")))?;
    sign_envelope_hex(secret_key_hex, &value).map_err(map_err)
}

// ---------------------------------------------------------------------------
// Stage C.2 — claim router
// ---------------------------------------------------------------------------

fn dict_get_str(d: &Bound<'_, PyDict>, key: &str) -> PyResult<Option<String>> {
    match d.get_item(key)? {
        None => Ok(None),
        Some(v) if v.is_none() => Ok(None),
        Some(v) => Ok(Some(v.extract::<String>()?)),
    }
}

fn dict_get_string_list(d: &Bound<'_, PyDict>, key: &str) -> PyResult<Vec<String>> {
    match d.get_item(key)? {
        None => Ok(Vec::new()),
        Some(v) if v.is_none() => Ok(Vec::new()),
        Some(v) => v.extract::<Vec<String>>(),
    }
}

fn extract_runner(runner: &Bound<'_, PyDict>) -> PyResult<RunnerView> {
    Ok(RunnerView::from_raw(
        &dict_get_string_list(runner, "scope_prefixes")?,
        &dict_get_string_list(runner, "tools")?,
        &dict_get_string_list(runner, "tags")?,
        dict_get_str(runner, "tenant")?,
        dict_get_str(runner, "workspace_root")?,
        dict_get_str(runner, "last_known_commit")?,
    ))
}

/// Hot-path matcher: borrows `&str` views into PyStrings (no `String` alloc),
/// short-circuits on the first failed gate, and skips field extraction when
/// the runner is unconstrained for that field.
fn task_matches(task: &Bound<'_, PyDict>, runner: &RunnerView) -> PyResult<bool> {
    // 1. Tenant gate.
    if let Some(t_obj) = task.get_item("tenant")? {
        if !t_obj.is_none() {
            let t: &str = t_obj.extract()?;
            match runner.tenant.as_deref() {
                Some(rt) if rt == t => {}
                _ => return Ok(false),
            }
        }
    }
    // 2. Workspace gate (skip if runner doesn't pin a workspace).
    if let Some(rw) = runner.workspace_root.as_deref() {
        if let Some(t_obj) = task.get_item("workspace_root")? {
            if !t_obj.is_none() {
                let t: &str = t_obj.extract()?;
                if rw != t {
                    return Ok(false);
                }
            }
        }
    }
    // 3. Scope prefix affinity (skip if runner has no prefixes).
    if !runner.scope_prefixes.is_empty() {
        if let Some(globs_obj) = task.get_item("scope_globs")? {
            if !globs_obj.is_none() {
                for item in globs_obj.try_iter()? {
                    let g_obj = item?;
                    let g: &str = g_obj.extract()?;
                    let head = fw_claim_router::glob_static_prefix(g);
                    let ok = runner
                        .scope_prefixes
                        .iter()
                        .any(|p| head.starts_with(p) || p.starts_with(&head));
                    if !ok {
                        return Ok(false);
                    }
                }
            }
        }
    }
    // 4. Required tools.
    if let Some(tl_obj) = task.get_item("required_tools")? {
        if !tl_obj.is_none() {
            for item in tl_obj.try_iter()? {
                let t_obj = item?;
                let t: &str = t_obj.extract()?;
                if !runner
                    .tools
                    .iter()
                    .any(|rt| rt.eq_ignore_ascii_case(t))
                {
                    return Ok(false);
                }
            }
        }
    }
    // 5. Required tags.
    if let Some(tg_obj) = task.get_item("required_tags")? {
        if !tg_obj.is_none() {
            for item in tg_obj.try_iter()? {
                let t_obj = item?;
                let t: &str = t_obj.extract()?;
                if !runner.tags.iter().any(|rt| rt.eq_ignore_ascii_case(t)) {
                    return Ok(false);
                }
            }
        }
    }
    // 6. Base-commit precondition.
    if let Some(rbc_obj) = task.get_item("require_base_commit")? {
        if !rbc_obj.is_none() && rbc_obj.extract::<bool>()? {
            // Python: reject if last_known is falsy or != task.base_commit.
            let rc = match runner.last_known_commit.as_deref() {
                Some(rc) if !rc.is_empty() => rc,
                _ => return Ok(false),
            };
            let bc_obj = task.get_item("base_commit")?;
            match bc_obj {
                Some(bc) if !bc.is_none() => {
                    let bc_str: &str = bc.extract()?;
                    if rc != bc_str {
                        return Ok(false);
                    }
                }
                _ => return Ok(false),
            }
        }
    }
    Ok(true)
}

#[pyfunction]
#[pyo3(signature = (tasks, runner))]
fn pick_task<'py>(
    py: Python<'py>,
    tasks: &Bound<'py, PyList>,
    runner: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyTuple>> {
    let runner_view = extract_runner(runner)?;
    let mut idx_match: Option<i64> = None;
    let mut seen: i64 = 0;
    for (i, item) in tasks.iter().enumerate() {
        seen += 1;
        let d = item
            .downcast::<PyDict>()
            .map_err(|_| PyValueError::new_err("each task must be a dict"))?;
        if task_matches(d, &runner_view)? {
            idx_match = Some(i as i64);
            break;
        }
    }
    let idx_py: PyObject = match idx_match {
        Some(i) => i.into_pyobject(py)?.into_any().unbind(),
        None => py.None(),
    };
    let seen_py: PyObject = seen.into_pyobject(py)?.into_any().unbind();
    PyTuple::new(py, [idx_py, seen_py])
}

// ---------------------------------------------------------------------------
// Stage C.3 — stream sequence counter
// ---------------------------------------------------------------------------

#[pyclass(name = "StreamCounter", module = "forgewire_runtime")]
struct PyStreamCounter {
    inner: StreamCounter,
}

#[pymethods]
impl PyStreamCounter {
    #[new]
    fn new() -> Self {
        Self {
            inner: StreamCounter::new(),
        }
    }

    /// Prime a task's counter from SQLite's current `MAX(seq)`.
    fn prime(&self, task_id: i64, current_max: u64) {
        self.inner.prime(task_id, current_max);
    }

    /// Return whether `task_id` has been primed.
    fn is_primed(&self, task_id: i64) -> bool {
        self.inner.is_primed(task_id)
    }

    /// Allocate the next sequence for `task_id`. Raises `LookupError` if
    /// the counter has not been primed.
    fn next_seq(&self, task_id: i64) -> PyResult<u64> {
        self.inner
            .next_seq(task_id)
            .ok_or_else(|| pyo3::exceptions::PyLookupError::new_err(format!(
                "stream counter for task {task_id} not primed"
            )))
    }

    /// Forget a task's counter. After this, [`next_seq`] requires re-priming.
    fn forget(&self, task_id: i64) {
        self.inner.forget(task_id);
    }

    /// Number of tasks with a live counter (diagnostic).
    fn task_count(&self) -> usize {
        self.inner.task_count()
    }
}

/// Module-level metadata.
#[pymodule]
fn forgewire_runtime(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("HAS_RUST", true)?;
    m.add_function(wrap_pyfunction!(verify_signature, m)?)?;
    m.add_function(wrap_pyfunction!(sign_payload, m)?)?;
    m.add_function(wrap_pyfunction!(canonicalize, m)?)?;
    m.add_function(wrap_pyfunction!(verify_envelope, m)?)?;
    m.add_function(wrap_pyfunction!(sign_envelope, m)?)?;
    m.add_function(wrap_pyfunction!(pick_task, m)?)?;
    m.add_class::<PyStreamCounter>()?;
    Ok(())
}
