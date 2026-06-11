import tempfile
import unittest
from pathlib import Path
from unittest import mock

from utils import massive_config


class MassiveConfigTests(unittest.TestCase):
    def tearDown(self):
        massive_config.get_massive_api_keys.cache_clear()

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
