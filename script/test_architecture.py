#!/usr/bin/env python3
"""Negative controls for the architectural boundaries enforced in CI."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "architecture", Path(__file__).with_name("check-architecture.py")
)
architecture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(architecture)


class ArchitectureTest(unittest.TestCase):
    def test_owned_process_and_independent_redaction(self):
        module = architecture.MODULE
        self.assertEqual([], architecture.violations([
            {"ImportPath": module, "Imports": ["net", "os", module + "/internal/redact"]},
            {"ImportPath": module + "/spawn", "Imports": [module, "os/exec"]},
            {"ImportPath": module + "/internal/redact", "Imports": ["bytes"]},
        ]))

    def test_rejects_business_dependencies_in_every_package(self):
        for owner in ("", "/spawn", "/internal/newhelper"):
            for app in architecture.APPLICATIONS:
                with self.subTest(owner=owner, application=app):
                    self.assertTrue(architecture.violations([
                        {"ImportPath": architecture.MODULE + owner,
                         "Imports": [app + "/connector"]},
                    ]))

    def test_session_cannot_own_spawn_and_redaction_cannot_own_session(self):
        module = architecture.MODULE
        for owner, dependency in ((module, module + "/spawn"),
                                  (module + "/internal/redact", module)):
            with self.subTest(owner=owner, dependency=dependency):
                self.assertTrue(architecture.violations([
                    {"ImportPath": owner, "Imports": [dependency]},
                ]))

    def test_internal_shim_cannot_hide_process_ownership(self):
        module = architecture.MODULE
        self.assertTrue(architecture.violations([
            {"ImportPath": module, "Imports": [module + "/internal/helper"],
             "Deps": [module + "/internal/helper", module + "/spawn"]},
        ]))


if __name__ == "__main__":
    unittest.main()
