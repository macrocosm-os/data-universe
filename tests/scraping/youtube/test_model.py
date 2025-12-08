import datetime as dt
import unittest

from scraping.youtube.model import YouTubeContent, normalize_channel_name


class TestNormalizeChannelName(unittest.TestCase):
    """Tests for the normalize_channel_name function."""

    def test_spaces_converted_to_underscores(self):
        """Spaces in channel names should be converted to underscores."""
        self.assertEqual(normalize_channel_name("Wicked Tuna"), "wicked_tuna")
        self.assertEqual(normalize_channel_name("Cold Fusion"), "cold_fusion")
        self.assertEqual(normalize_channel_name("Two Minute Papers"), "two_minute_papers")

    def test_hyphens_converted_to_underscores(self):
        """Hyphens in channel names should be converted to underscores."""
        self.assertEqual(normalize_channel_name("Wicked-Tuna"), "wicked_tuna")
        self.assertEqual(normalize_channel_name("Cold-Fusion"), "cold_fusion")

    def test_underscores_preserved(self):
        """Existing underscores should be preserved."""
        self.assertEqual(normalize_channel_name("Wicked_Tuna"), "wicked_tuna")
        self.assertEqual(normalize_channel_name("already_underscored"), "already_underscored")

    def test_mixed_separators(self):
        """Mixed separators (spaces, hyphens, underscores) should all become underscores."""
        self.assertEqual(normalize_channel_name("Some-Channel Name_Here"), "some_channel_name_here")
        self.assertEqual(normalize_channel_name("A-B C_D"), "a_b_c_d")

    def test_consecutive_separators(self):
        """Consecutive separators should be collapsed to single underscore."""
        self.assertEqual(normalize_channel_name("Multiple   Spaces"), "multiple_spaces")
        self.assertEqual(normalize_channel_name("Multiple---Hyphens"), "multiple_hyphens")
        self.assertEqual(normalize_channel_name("Mixed--  -Separators"), "mixed_separators")

    def test_simple_names_unchanged(self):
        """Simple names without separators should remain unchanged (lowercased)."""
        self.assertEqual(normalize_channel_name("Fireship"), "fireship")
        self.assertEqual(normalize_channel_name("lexfridman"), "lexfridman")
        self.assertEqual(normalize_channel_name("MSNBC"), "msnbc")

    def test_empty_and_whitespace(self):
        """Empty or whitespace-only names should return 'unknown'."""
        self.assertEqual(normalize_channel_name(""), "unknown")
        self.assertEqual(normalize_channel_name("   "), "unknown")
        self.assertEqual(normalize_channel_name(None), "unknown")

    def test_max_length_truncation(self):
        """Names exceeding max_len should be truncated."""
        long_name = "a" * 100
        result = normalize_channel_name(long_name, max_len=50)
        self.assertEqual(len(result), 50)

    def test_leading_trailing_separators_stripped(self):
        """Leading and trailing separators should be stripped."""
        self.assertEqual(normalize_channel_name(" Channel Name "), "channel_name")
        self.assertEqual(normalize_channel_name("-Channel-"), "channel")
        self.assertEqual(normalize_channel_name("_Channel_"), "channel")


class TestYouTubeContentLabelMethods(unittest.TestCase):
    """Tests for YouTubeContent label-related static methods."""

    def test_create_channel_label(self):
        """create_channel_label should produce consistent #ytc_c_ prefixed labels."""
        self.assertEqual(
            YouTubeContent.create_channel_label("Wicked Tuna"),
            "#ytc_c_wicked_tuna"
        )
        self.assertEqual(
            YouTubeContent.create_channel_label("lexfridman"),
            "#ytc_c_lexfridman"
        )
        self.assertEqual(
            YouTubeContent.create_channel_label("@lexfridman"),
            "#ytc_c_lexfridman"
        )

    def test_normalize_label_for_comparison_converts_hyphens(self):
        """normalize_label_for_comparison should convert hyphens to underscores in the slug."""
        self.assertEqual(
            YouTubeContent.normalize_label_for_comparison("#ytc_c_wicked-tuna"),
            "#ytc_c_wicked_tuna"
        )
        self.assertEqual(
            YouTubeContent.normalize_label_for_comparison("#ytc_c_cold-fusion"),
            "#ytc_c_cold_fusion"
        )

    def test_normalize_label_for_comparison_preserves_underscores(self):
        """normalize_label_for_comparison should preserve already-underscored labels."""
        self.assertEqual(
            YouTubeContent.normalize_label_for_comparison("#ytc_c_wicked_tuna"),
            "#ytc_c_wicked_tuna"
        )

    def test_normalize_label_for_comparison_non_youtube_labels(self):
        """normalize_label_for_comparison should return non-YouTube labels unchanged."""
        self.assertEqual(
            YouTubeContent.normalize_label_for_comparison("r/Bitcoin"),
            "r/Bitcoin"
        )
        self.assertEqual(
            YouTubeContent.normalize_label_for_comparison("#bitcoin"),
            "#bitcoin"
        )
        self.assertEqual(
            YouTubeContent.normalize_label_for_comparison(""),
            ""
        )

    def test_labels_match_identical_labels(self):
        """labels_match should return True for identical labels."""
        self.assertTrue(
            YouTubeContent.labels_match("#ytc_c_wicked_tuna", "#ytc_c_wicked_tuna")
        )
        self.assertTrue(
            YouTubeContent.labels_match("#ytc_c_lexfridman", "#ytc_c_lexfridman")
        )

    def test_labels_match_hyphen_vs_underscore(self):
        """labels_match should return True when only difference is hyphen vs underscore.

        This is the key backwards compatibility feature:
        - Old miners produce: #ytc_c_wicked-tuna (hyphens from spaces)
        - New miners/jobs produce: #ytc_c_wicked_tuna (underscores)
        - Both should match for validation to pass.
        """
        self.assertTrue(
            YouTubeContent.labels_match("#ytc_c_wicked-tuna", "#ytc_c_wicked_tuna")
        )
        self.assertTrue(
            YouTubeContent.labels_match("#ytc_c_wicked_tuna", "#ytc_c_wicked-tuna")
        )
        self.assertTrue(
            YouTubeContent.labels_match("#ytc_c_cold-fusion", "#ytc_c_cold_fusion")
        )
        self.assertTrue(
            YouTubeContent.labels_match("#ytc_c_two-minute-papers", "#ytc_c_two_minute_papers")
        )

    def test_labels_match_different_channels(self):
        """labels_match should return False for different channel slugs."""
        self.assertFalse(
            YouTubeContent.labels_match("#ytc_c_channel_a", "#ytc_c_channel_b")
        )
        self.assertFalse(
            YouTubeContent.labels_match("#ytc_c_lexfridman", "#ytc_c_coinbureau")
        )

    def test_labels_match_empty_strings(self):
        """labels_match should handle empty strings."""
        self.assertTrue(YouTubeContent.labels_match("", ""))
        self.assertFalse(YouTubeContent.labels_match("", "#ytc_c_channel"))
        self.assertFalse(YouTubeContent.labels_match("#ytc_c_channel", ""))

    def test_labels_match_non_youtube_labels(self):
        """labels_match should work with non-YouTube labels (exact match only)."""
        self.assertTrue(YouTubeContent.labels_match("r/Bitcoin", "r/Bitcoin"))
        self.assertFalse(YouTubeContent.labels_match("r/Bitcoin", "r/Ethereum"))

    def test_parse_channel_label(self):
        """parse_channel_label should extract channel slug from valid labels."""
        # Should work with both hyphens and underscores
        self.assertEqual(
            YouTubeContent.parse_channel_label("#ytc_c_wicked_tuna"),
            "wicked_tuna"
        )
        self.assertEqual(
            YouTubeContent.parse_channel_label("#ytc_c_wicked-tuna"),
            "wicked-tuna"
        )
        self.assertEqual(
            YouTubeContent.parse_channel_label("#ytc_c_lexfridman"),
            "lexfridman"
        )

    def test_parse_channel_label_invalid(self):
        """parse_channel_label should return None for invalid labels."""
        self.assertIsNone(YouTubeContent.parse_channel_label("r/Bitcoin"))
        self.assertIsNone(YouTubeContent.parse_channel_label("#bitcoin"))
        self.assertIsNone(YouTubeContent.parse_channel_label(""))


class TestYouTubeContentToDataEntity(unittest.TestCase):
    """Tests for YouTubeContent.to_data_entity conversion."""

    def test_to_data_entity_label_uses_underscores(self):
        """to_data_entity should create labels with underscores (not hyphens)."""
        content = YouTubeContent(
            video_id="abc123",
            title="Test Video",
            channel_name="Wicked Tuna",  # Has space
            upload_date=dt.datetime.now(dt.timezone.utc),
            transcript=[],
            url="https://www.youtube.com/watch?v=abc123",
            duration_seconds=100,
            thumbnails="https://i.ytimg.com/vi/abc123/hqdefault.jpg",
            view_count=1000,
            like_count=100,
        )

        entity = YouTubeContent.to_data_entity(content)

        # Label should use underscores
        self.assertEqual(entity.label.value, "#ytc_c_wicked_tuna")

    def test_to_data_entity_label_handles_hyphens_in_name(self):
        """to_data_entity should convert hyphens in channel names to underscores."""
        content = YouTubeContent(
            video_id="abc123",
            title="Test Video",
            channel_name="Cold-Fusion",  # Has hyphen
            upload_date=dt.datetime.now(dt.timezone.utc),
            transcript=[],
            url="https://www.youtube.com/watch?v=abc123",
            duration_seconds=100,
            thumbnails="https://i.ytimg.com/vi/abc123/hqdefault.jpg",
            view_count=1000,
            like_count=100,
        )

        entity = YouTubeContent.to_data_entity(content)

        # Label should use underscores
        self.assertEqual(entity.label.value, "#ytc_c_cold_fusion")


class TestBackwardsCompatibilityScenarios(unittest.TestCase):
    """
    Integration tests for backwards compatibility scenarios.

    These tests document the real-world scenarios where miners with old code
    (producing hyphens) need to match against new job configs (using underscores).
    """

    def test_old_miner_label_matches_new_job_label(self):
        """
        Scenario: Old miner produces hyphen-based label, new job config has underscore-based label.

        Old miner code: "Wicked Tuna" -> "wicked-tuna" -> "#ytc_c_wicked-tuna"
        New job config: "Wicked Tuna" -> "wicked_tuna" -> "#ytc_c_wicked_tuna"

        Result: Should match via labels_match()
        """
        old_miner_label = "#ytc_c_wicked-tuna"  # Produced by old normalize_channel_name
        new_job_label = "#ytc_c_wicked_tuna"    # Produced by new normalize_channel_name

        self.assertTrue(
            YouTubeContent.labels_match(old_miner_label, new_job_label),
            "Old miner labels (hyphens) should match new job labels (underscores)"
        )

    def test_new_miner_label_matches_new_job_label(self):
        """
        Scenario: New miner produces underscore-based label, new job config has underscore-based label.

        Both use the new normalize_channel_name producing underscores.

        Result: Should match exactly.
        """
        new_miner_label = "#ytc_c_wicked_tuna"
        new_job_label = "#ytc_c_wicked_tuna"

        self.assertTrue(
            YouTubeContent.labels_match(new_miner_label, new_job_label),
            "New miner labels should match new job labels"
        )

    def test_complex_channel_name_backwards_compatibility(self):
        """Test backwards compatibility with complex channel names."""
        test_cases = [
            ("Two Minute Papers", "#ytc_c_two-minute-papers", "#ytc_c_two_minute_papers"),
            ("AI Crypto TV", "#ytc_c_ai-crypto-tv", "#ytc_c_ai_crypto_tv"),
            ("Some-Hyphenated-Channel", "#ytc_c_some-hyphenated-channel", "#ytc_c_some_hyphenated_channel"),
        ]

        for channel_name, old_label, new_label in test_cases:
            with self.subTest(channel_name=channel_name):
                self.assertTrue(
                    YouTubeContent.labels_match(old_label, new_label),
                    f"Old label {old_label} should match new label {new_label}"
                )


if __name__ == "__main__":
    unittest.main()
