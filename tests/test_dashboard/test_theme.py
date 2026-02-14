"""テーマモジュールのテスト。"""

import pytest

from src.dashboard.components.theme import (
    ACCENT_BLUE,
    ACCENT_GREEN,
    ACCENT_RED,
    ACCENT_YELLOW,
    BG_PRIMARY,
    BG_SECONDARY,
    BG_TERTIARY,
    BORDER,
    TEXT_PRIMARY,
    TEXT_SECONDARY,
)


class TestThemeConstants:
    """テーマ定数のテスト。"""

    def test_colors_are_hex(self) -> None:
        """全色がHEXカラーコードであること。"""
        colors = [
            BG_PRIMARY, BG_SECONDARY, BG_TERTIARY,
            TEXT_PRIMARY, TEXT_SECONDARY,
            ACCENT_BLUE, ACCENT_GREEN, ACCENT_RED, ACCENT_YELLOW,
            BORDER,
        ]
        for c in colors:
            assert c.startswith("#"), f"{c} is not a hex color"
            assert len(c) == 7, f"{c} should be #RRGGBB format"

    def test_primary_colors_are_dark(self) -> None:
        """背景色がダークテーマ用の暗い色であること。"""
        # 背景色のR成分が0x30未満（暗い色）
        r_value = int(BG_PRIMARY[1:3], 16)
        assert r_value < 0x30

    def test_text_is_light(self) -> None:
        """テキスト色が明るい色であること。"""
        r_value = int(TEXT_PRIMARY[1:3], 16)
        assert r_value > 0xC0
