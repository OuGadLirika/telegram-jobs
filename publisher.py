from __future__ import annotations

import io


async def send_post_with_photo(bot, chat_id, text: str, banner_path: str) -> None:
    with open(banner_path, "rb") as photo_file:
        payload = photo_file.read()
    await bot.send_photo(
        chat_id=chat_id,
        photo=io.BytesIO(payload),
        caption=text,
        parse_mode="Markdown",
    )
