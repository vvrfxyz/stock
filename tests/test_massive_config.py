import tempfile
import unittest
from pathlib import Path
from unittest import mock

from utils import massive_config


class MassiveConfigTests(unittest.TestCase):
    def tearDown(self):
        massive_config.get_massive_api_keys.cache_clear()

    def test_allowed_us_security_types_locked(self):
        # ADR 全量入库方案（docs/adr_expansion_plan_2026-07.md §B）锁定该常量内容：
        # 任何增删类型都会波及 cleanup_us_universe 的物理 DELETE 等 8 个引用方，
        # 必须显式改这里的断言以确认知晓后果。
        self.assertEqual(
            set(massive_config.ALLOWED_US_SECURITY_TYPES),
            {"CS", "ETF", "ADRC", "ADRP", "ADRR"},
        )
        self.assertEqual(len(massive_config.ALLOWED_US_SECURITY_TYPES), 5)

    def test_is_supported_us_security_type_membership(self):
        for type_code in ("CS", "ETF", "ADRC", "ADRP", "ADRR"):
            self.assertTrue(massive_config.is_supported_us_security_type(type_code))
            self.assertTrue(massive_config.is_supported_us_security_type(type_code.lower()))
        for type_code in ("PFD", "WARRANT", "UNIT", "FUND", "SP", "", None):
            self.assertFalse(massive_config.is_supported_us_security_type(type_code))

    def test_reads_keys_from_activation_value_file(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            key_file = Path(tmp_dir) / "activation_value.txt"
            key_file.write_text("key-a\n# comment\nkey-b,key-c\n\n", encoding="utf-8")

            with mock.patch.object(massive_config, "MASSIVE_KEYS_FILE", key_file):
                massive_config.get_massive_api_keys.cache_clear()
                self.assertEqual(
                    massive_config.get_massive_api_keys(),
                    ["key-a", "key-b", "key-c"],
                )

    def test_does_not_fallback_to_env_when_file_missing(self):
        missing_file = Path("/tmp/definitely-missing-activation-value.txt")

        with mock.patch.object(massive_config, "MASSIVE_KEYS_FILE", missing_file):
            with mock.patch.dict("os.environ", {"MASSIVE_API_KEYS": "env-key"}, clear=False):
                massive_config.get_massive_api_keys.cache_clear()
                with self.assertRaisesRegex(ValueError, "key 文件不存在"):
                    massive_config.get_massive_api_keys()


if __name__ == "__main__":
    unittest.main()
