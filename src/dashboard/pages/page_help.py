"""Tab 7: ユーザーマニュアルページ。

docs/user_manual.md を読み込んでマークダウン表示する。
"""

from pathlib import Path

import streamlit as st

st.header("ユーザーマニュアル")

manual_path = (
    Path(__file__).resolve().parent.parent.parent.parent / "docs" / "user_manual.md"
)

if manual_path.exists():
    content = manual_path.read_text(encoding="utf-8")
    st.markdown(content)
else:
    st.warning(
        f"ユーザーマニュアルが見つかりません。\n\n"
        f"期待するパス: `{manual_path}`"
    )
