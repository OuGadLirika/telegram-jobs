from __future__ import annotations

import asyncio
from datetime import datetime

from telegram import Update
from telegram.ext import ContextTypes

from parser import extract_job_id_from_message_text, format_post, job_fingerprint
from publisher import send_post_with_photo as send_post_with_photo_v2
from queue_manager import now_ts
from storage import QueueItem


class BotHandlers:
    def __init__(self, service):
        self.service = service

    async def fetch_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_user and update.effective_user.id != self.service.settings.admin_user_id:
            await update.message.reply_text("⛔️ Нет доступа.")
            return

        await update.message.reply_text("🔎 Запускаю ручной RSS-цикл...")
        metrics = await self.service.run_rss_cycle()
        self.service.log_event("admin_fetch", admin_id=update.effective_user.id, **metrics)
        await update.message.reply_text(
            (
                "✅ Ручной RSS-цикл завершен.\n"
                f"fetched={metrics.get('fetched', 0)}\n"
                f"queued={metrics.get('queued', 0)}\n"
                f"skipped={metrics.get('skipped', 0)}\n"
                f"failed={metrics.get('failed', 0)}"
            )
        )

    async def info_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_user and update.effective_user.id != self.service.settings.admin_user_id:
            await update.message.reply_text("⛔️ Нет доступа.")
            return

        items = await asyncio.to_thread(self.service.repo.queue_items)
        if not items:
            await update.message.reply_text("Очередь пуста.")
            return

        # Keep a safe margin below Telegram's 4096-char limit.
        max_message_len = 3500
        header = "Очередь публикации:"
        continuation_header = "Очередь публикации (продолжение):"
        lines = [header]
        for item in items:
            dt = datetime.fromtimestamp(item.scheduled_at).strftime("%Y-%m-%d %H:%M:%S")
            line = f"#{item.id} job_id={item.job_id} scheduled={dt}"
            candidate = "\n".join(lines + [line])
            if len(candidate) > max_message_len:
                await update.message.reply_text("\n".join(lines))
                lines = [continuation_header, line]
            else:
                lines.append(line)

        await update.message.reply_text("\n".join(lines))

    async def delete_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_user and update.effective_user.id != self.service.settings.admin_user_id:
            await update.message.reply_text("⛔️ Нет доступа.")
            return

        arg = context.args[0] if context.args else "next"
        target_item: QueueItem | None = None

        if arg == "next":
            items = await asyncio.to_thread(self.service.repo.queue_items)
            target_item = items[0] if items else None
        elif arg.isdigit():
            target_item = await asyncio.to_thread(self.service.repo.get_queued_item_by_id, int(arg))
        else:
            target_item = await asyncio.to_thread(self.service.repo.get_queued_item_by_job_id, arg)

        if not target_item:
            await update.message.reply_text("❌ Элемент очереди не найден.")
            return

        await asyncio.to_thread(self.service.repo.mark_queue_deleted, target_item.id, "deleted_by_admin")
        self.service.log_event(
            "admin_delete",
            admin_id=update.effective_user.id,
            queue_id=target_item.id,
            job_id=target_item.job_id,
        )
        await update.message.reply_text(f"🗑️ Удалено: queue_id={target_item.id}, job_id={target_item.job_id}")

    async def push_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_user and update.effective_user.id != self.service.settings.admin_user_id:
            await update.message.reply_text("⛔️ Нет доступа.")
            return

        arg = context.args[0] if context.args else "next"
        target_item: QueueItem | None = None

        if arg == "next":
            items = await asyncio.to_thread(self.service.repo.queue_items)
            target_item = items[0] if items else None
        elif arg.isdigit():
            target_item = await asyncio.to_thread(self.service.repo.get_queued_item_by_id, int(arg))
        else:
            target_item = await asyncio.to_thread(self.service.repo.get_queued_item_by_job_id, arg)

        if not target_item:
            await update.message.reply_text("❌ Элемент очереди не найден.")
            return

        await asyncio.to_thread(self.service.repo.mark_queue_processing, target_item.id)
        try:
            await send_post_with_photo_v2(
                context.bot,
                self.service.settings.target_channel,
                target_item.text,
                self.service.settings.banner_path,
            )
            posted_at = now_ts()
            await asyncio.to_thread(self.service.repo.mark_queue_posted, target_item.id, posted_at)
            await asyncio.to_thread(
                self.service.repo.upsert_post_status,
                target_item.job_id,
                target_item.text,
                "sent",
                posted_at,
                None,
            )
            self.service.log_event(
                "admin_push",
                admin_id=update.effective_user.id,
                queue_id=target_item.id,
                job_id=target_item.job_id,
            )
            await update.message.reply_text(f"✅ Опубликовано: queue_id={target_item.id}, job_id={target_item.job_id}")
        except Exception as exc:
            await asyncio.to_thread(self.service.repo.mark_queue_failed, target_item.id, str(exc))
            await update.message.reply_text(f"❌ Ошибка публикации: {exc}")

    async def reschedule_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_user and update.effective_user.id != self.service.settings.admin_user_id:
            await update.message.reply_text("⛔️ Нет доступа.")
            return

        interval = self.service.settings.send_interval_seconds
        if context.args:
            if not context.args[0].isdigit() or int(context.args[0]) <= 0:
                await update.message.reply_text("❌ Использование: /reschedule [seconds]")
                return
            interval = int(context.args[0])

        total = await asyncio.to_thread(self.service.repo.reschedule_queue, interval, now_ts())
        if total == 0:
            await update.message.reply_text("Очередь пуста.")
            return

        self.service.log_event(
            "admin_reschedule",
            admin_id=update.effective_user.id,
            interval_seconds=interval,
            queued=total,
        )
        await update.message.reply_text(
            f"✅ Очередь пересчитана: {total} элементов, шаг {interval} сек."
        )

    async def text_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_user and update.effective_user.id != self.service.settings.admin_user_id:
            return
        if not update.message or not update.message.text or not self.service.is_affirmative(update.message.text):
            return

        reply = update.message.reply_to_message
        if not reply:
            await update.message.reply_text("❌ Ответьте на сообщение с вакансией.")
            return

        job_id = extract_job_id_from_message_text(reply.text or reply.caption or "")
        if not job_id:
            await update.message.reply_text("❌ Не удалось определить job_id из сообщения.")
            return

        job = await asyncio.to_thread(self.service.repo.get_job, job_id)
        if not job:
            await update.message.reply_text("❌ Вакансия не найдена в базе.")
            return

        link = await asyncio.to_thread(self.service.repo.get_apply_link, job_id) or job.get("url", "")
        fingerprint = job.get("fingerprint") or job_fingerprint(
            job.get("title", ""),
            job.get("company", ""),
            link,
            job.get("location", ""),
        )
        post = format_post(job, link)
        decision = await asyncio.to_thread(
            self.service.queue.enqueue,
            job_id=job_id,
            text=post,
            fingerprint=fingerprint,
            source="admin-confirmation",
        )
        if decision.queued:
            await asyncio.to_thread(self.service.repo.upsert_post_status, job_id, post, "delayed", None, None)
            await update.message.reply_text("⏳ Пост добавлен в очередь.")
        else:
            await update.message.reply_text(f"⚠️ Не добавлено в очередь: {decision.reason}")
