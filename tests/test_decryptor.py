import importlib
import inspect
from pathlib import Path
import sys
import types
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))



class _FakePasswordError(Exception):
    pass


class _FakePDF:
    def __init__(self):
        self.saved_paths = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def save(self, path):
        self.saved_paths.append(path)

    def close(self):
        return None


# ── helper ──────────────────────────────────────────────────────────────────

def _import_decryptor_with_fake_pikepdf(open_impl):
    """Return the decryptor module with pikepdf stubbed out.

    os.path.exists is patched to always return True so that the
    file-existence guard does not block tests that use synthetic paths.
    """
    fake_pikepdf = types.SimpleNamespace(
        PasswordError=_FakePasswordError,
        open=open_impl,
    )
    sys.modules["pikepdf"] = fake_pikepdf
    sys.modules.pop("decryptor", None)
    module = importlib.import_module("decryptor")
    patcher = patch.object(module.os.path, "exists", return_value=True)
    patcher.start()
    return module, patcher


# ── tests ────────────────────────────────────────────────────────────────────

def test_import_has_no_hardcoded_execution():
    calls = {"count": 0}

    def open_impl(*args, **kwargs):
        calls["count"] += 1
        return _FakePDF()

    module, patcher = _import_decryptor_with_fake_pikepdf(open_impl)
    try:
        assert calls["count"] == 0
    finally:
        patcher.stop()


def test_module_docstring_describes_scope():
    module, patcher = _import_decryptor_with_fake_pikepdf(
        lambda *a, **k: _FakePDF()
    )
    try:
        module_doc = inspect.getdoc(module) or ""
        assert "decrypt" in module_doc.lower()
        assert "does not generate passwords" in module_doc.lower()
        assert "does not retry" in module_doc.lower()
        assert "single attempt" in module_doc.lower()
    finally:
        patcher.stop()


def test_not_encrypted_pdf_returns_expected_failure_shape():
    module, patcher = _import_decryptor_with_fake_pikepdf(
        lambda *a, **k: _FakePDF()
    )
    try:
        result = module.decrypt_pdf("input.pdf", "secret", "out.pdf")
        assert result == {
            "status": "failure",
            "failure_reason": "PDF_NOT_ENCRYPTED",
            "attempts": 1,
        }
    finally:
        patcher.stop()


def test_wrong_password_returns_expected_failure_shape():
    def open_impl(path, password=None):
        raise _FakePasswordError()

    module, patcher = _import_decryptor_with_fake_pikepdf(open_impl)
    try:
        result = module.decrypt_pdf("input.pdf", "wrong", "out.pdf")
        assert result == {
            "status": "failure",
            "failure_reason": "WRONG_PASSWORD",
            "attempts": 1,
        }
    finally:
        patcher.stop()


def test_correct_password_returns_success_and_saves_output():
    saved = {"path": None}

    class SavingPDF(_FakePDF):
        def save(self, path):
            saved["path"] = path
            super().save(path)

    def open_impl(path, password=None):
        if password is None:
            raise _FakePasswordError()
        return SavingPDF()

    module, patcher = _import_decryptor_with_fake_pikepdf(open_impl)
    try:
        result = module.decrypt_pdf("input.pdf", "right", "decrypted.pdf")
        assert result == {
            "status": "success",
            "output_path": "decrypted.pdf",
            "attempts": 1,
        }
        assert saved["path"] == "decrypted.pdf"
    finally:
        patcher.stop()


def test_missing_file_returns_file_not_found_failure():
    """When os.path.exists returns False the guard must fire immediately."""
    module, patcher = _import_decryptor_with_fake_pikepdf(
        lambda *a, **k: _FakePDF()
    )
    try:
        with patch.object(module.os.path, "exists", return_value=False):
            result = module.decrypt_pdf("missing.pdf", "pw", "out.pdf")
        assert result == {
            "status": "failure",
            "failure_reason": "FILE_NOT_FOUND",
            "attempts": 0,
        }
    finally:
        patcher.stop()
