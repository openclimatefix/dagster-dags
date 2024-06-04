import unittest

from containers.icon.download_combine_upload_icon import *


class TestFindUrl(unittest.TestCase):

    def test_2d(self):
        urls = find_file_name(config=EUROPE_CONFIG, run_string="00")
        print([u for u in urls if "pressure" in u])
        self.assertTrue(False)
