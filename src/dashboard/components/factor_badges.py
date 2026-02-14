"""ファクターソース別バッジ表示コンポーネント。"""

from __future__ import annotations

# source値 -> (ラベル, 背景色, ツールチップ)
SOURCE_BADGES: dict[str, tuple[str, str, str]] = {
    "gy_initial": (
        "\u521d\u671f", "#58A6FF", "GY\u6307\u6570\u65b9\u5f0f\u306e\u521d\u671f\u30d5\u30a1\u30af\u30bf\u30fc",
    ),
    "discovery": (
        "\u63a2\u7d22", "#3FB950", "\u30c7\u30fc\u30bf\u30c9\u30ea\u30d6\u30f3\u767a\u898b\u3067\u8ffd\u52a0",
    ),
    "manual": ("\u624b\u52d5", "#D29922", "\u30e6\u30fc\u30b6\u30fc\u304c\u624b\u52d5\u4f5c\u6210"),
    "ai_generated": ("AI", "#F85149", "AI\u30a8\u30fc\u30b8\u30a7\u30f3\u30c8\u304c\u63d0\u6848"),
    "research": ("\u7814\u7a76", "#8B949E", "\u30ea\u30b5\u30fc\u30c1\u30d9\u30fc\u30b9\u3067\u4f5c\u6210"),
}


def source_badge_html(source: str) -> str:
    """ソースに応じたバッジHTMLを返す。"""
    label, color, tooltip = SOURCE_BADGES.get(
        source, ("\u4e0d\u660e", "#8B949E", "\u30bd\u30fc\u30b9\u4e0d\u660e")
    )
    return (
        f'<span style="background-color:{color};color:#fff;'
        f"padding:2px 8px;border-radius:4px;font-size:0.75rem;"
        f'font-weight:600;" title="{tooltip}">{label}</span>'
    )


def source_label(source: str) -> str:
    """ソースのラベル文字列を返す（Markdown用）。"""
    label, _, _ = SOURCE_BADGES.get(source, ("\u4e0d\u660e", "", ""))
    return label


def source_emoji(source: str) -> str:
    """ソースに応じた絵文字プレフィクスを返す。"""
    mapping = {
        "gy_initial": "\U0001f535",
        "discovery": "\U0001f7e2",
        "manual": "\U0001f7e1",
        "ai_generated": "\U0001f534",
        "research": "\u26aa",
    }
    return mapping.get(source, "\u26ab")
