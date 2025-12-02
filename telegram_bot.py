import logging
import os
from typing import Dict, Any, Set

logger = logging.getLogger(__name__)

# Configure telegram library loggers to hide sensitive data
telegram_logger = logging.getLogger('telegram')
telegram_logger.setLevel(logging.WARNING)
telegram_logger.propagate = True

# Disable httpx detailed logging to prevent token leakage
httpx_logger = logging.getLogger('httpx')
httpx_logger.setLevel(logging.WARNING)

# Disable httpcore detailed logging
httpcore_logger = logging.getLogger('httpcore')
httpcore_logger.setLevel(logging.WARNING)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

_telegram_enabled = False
_bot = None
_pending_auth: Dict[int, str] = {}
_pending_operations: Dict[int, Dict[str, Any]] = {}

try:
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        from telegram import Bot
        
        logger.info("Telegram configuration found")
        logger.info(f"Bot token length: {len(TELEGRAM_BOT_TOKEN)} chars")
        logger.info(f"Chat ID configured: {TELEGRAM_CHAT_ID[:3]}...")
        
        _bot = Bot(token=TELEGRAM_BOT_TOKEN)
        _telegram_enabled = True
        logger.info("Telegram bot client initialized successfully")
    else:
        missing = []
        if not TELEGRAM_BOT_TOKEN:
            missing.append("TELEGRAM_BOT_TOKEN")
        if not TELEGRAM_CHAT_ID:
            missing.append("TELEGRAM_CHAT_ID")
        logger.info(f"Telegram not configured. Missing: {', '.join(missing)}")
except ImportError:
    logger.warning("python-telegram-bot not installed. Install with: pip install python-telegram-bot")
except Exception as e:
    logger.error(f"Telegram init failed: {e}")


def _send_message(message: str, critical: bool = False):
    """Send simple notification message."""
    if not _telegram_enabled or not _bot:
        return
    
    try:
        import asyncio
        
        prefix = "🚨 CRITICAL" if critical else "ℹ️ INFO"
        
        async def send():
            await _bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=f"{prefix}\n\n{message}",
                parse_mode="Markdown"
            )
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(send())
        loop.close()
        
    except Exception as e:
        logger.error(f"Send message failed: {e}")


async def _show_main_menu(update: Any, authenticated: bool = False):
    """Show main menu with available commands."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    
    if authenticated:
        keyboard = [
            [InlineKeyboardButton("📊 System Status", callback_data="status")],
            [InlineKeyboardButton("📋 List SSH Hosts", callback_data="list_ssh")],
            [InlineKeyboardButton("📋 List API Hosts", callback_data="list_api")],
            [InlineKeyboardButton("➕ Add SSH Host", callback_data="add_ssh")],
            [InlineKeyboardButton("➕ Add API Host", callback_data="add_api")],
            [InlineKeyboardButton("🗑️ Remove SSH Host", callback_data="remove_ssh")],
            [InlineKeyboardButton("🗑️ Remove API Host", callback_data="remove_api")],
            [InlineKeyboardButton("🔴 Emergency Shutdown (All)", callback_data="shutdown")],
            [InlineKeyboardButton("⚡ Selective Shutdown", callback_data="selective_shutdown")],
            [InlineKeyboardButton("🔓 Logout", callback_data="logout")]
        ]
        text = "🛡️ Control Panel (Authenticated)\n\nSelect an operation:"
    else:
        keyboard = [[InlineKeyboardButton("🔐 Login", callback_data="login")]]
        text = "🛡️ Dead Man's Switch\n\nAuthenticate to access control panel."
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if hasattr(update, 'callback_query') and update.callback_query:
        try:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        except Exception:
            # Ignore "Message is not modified" errors when content is identical
            pass
    else:
        await update.message.reply_text(text, reply_markup=reply_markup)


async def _cmd_start(update: Any, context: Any):
    """Handle /start command."""
    user_id = update.effective_user.id
    
    if str(user_id) != TELEGRAM_CHAT_ID:
        await update.message.reply_text("⛔ Unauthorized")
        logger.warning(f"Unauthorized /start from user {user_id}")
        return
    
    from database import is_telegram_session_valid
    authenticated = is_telegram_session_valid(user_id)
    await _show_main_menu(update, authenticated)


async def _cmd_status(update: Any, context: Any):
    user_id = update.effective_user.id
    
    if str(user_id) != TELEGRAM_CHAT_ID:
        await update.message.reply_text("⛔ Unauthorized")
        return
    
    try:
        from database import get_all_ssh_hosts, get_all_api_hosts
        from dms_logic import is_shutdown_in_progress, get_shutdown_status
        
        text = "📊 **System Status**\n\n"
        
        if is_shutdown_in_progress():
            status = get_shutdown_status()
            text += f"⚠️ **SHUTDOWN IN PROGRESS**\n"
            text += f"Phase: {status.get('phase', 'unknown')}\n"
            text += f"Started: {status.get('started_at', 'N/A')}\n\n"
        else:
            text += "✅ **System Operational**\n\n"
        
        ssh_hosts = get_all_ssh_hosts(enabled_only=False)
        api_hosts = get_all_api_hosts(enabled_only=False)
        
        ssh_online = sum(1 for h in ssh_hosts if h.get('last_status') == 'online' and h['enabled'])
        ssh_total = sum(1 for h in ssh_hosts if h['enabled'])
        
        api_online = sum(1 for h in api_hosts if h.get('last_status') == 'online' and h['enabled'])
        api_total = sum(1 for h in api_hosts if h['enabled'])
        
        text += f"**SSH:** {ssh_online}/{ssh_total} online\n"
        text += f"**API:** {api_online}/{api_total} online\n"
        
        await update.message.reply_text(text, parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")


async def _cmd_logout(update: Any, context: Any):
    user_id = update.effective_user.id
    
    from database import remove_telegram_session, is_telegram_session_valid
    
    if is_telegram_session_valid(user_id):
        remove_telegram_session(user_id)
        await update.message.reply_text("✅ Logged out")
        logger.info(f"User {user_id} logged out")
    else:
        await update.message.reply_text("ℹ️ Not authenticated")


async def _button_callback(update: Any, context: Any):
    """Handle button callbacks."""
    query = update.callback_query
    try:
        await query.answer()
    except Exception:
        pass  # Ignore timeout on answer()
    
    user_id = update.effective_user.id
    
    if str(user_id) != TELEGRAM_CHAT_ID:
        await query.edit_message_text("⛔ Unauthorized")
        return
    
    from database import is_telegram_session_valid, remove_telegram_session
    
    data = query.data
    authenticated = is_telegram_session_valid(user_id)
    
    if data == "login":
        _pending_auth[user_id] = "awaiting_token"
        await query.edit_message_text(
            "🔐 Authentication (Step 1/2)\n\n"
            "Send your API static token.\n\n"
            "Note: TOTP code will be required next."
        )
        return
    
    if data == "back":
        await _show_main_menu(update, True)
        return
    
    if data == "logout":
        if authenticated:
            remove_telegram_session(user_id)
            logger.info(f"User {user_id} logged out")
        await _show_main_menu(update, False)
        return
    
    if not authenticated:
        await query.edit_message_text("⛔ Please login first")
        return
    
    if data == "status":
        try:
            from database import get_all_ssh_hosts, get_all_api_hosts
            from dms_logic import is_shutdown_in_progress, get_shutdown_status
            
            text = "📊 **System Status**\n\n"
            
            if is_shutdown_in_progress():
                status = get_shutdown_status()
                text += f"⚠️ **SHUTDOWN IN PROGRESS**\n"
                text += f"Phase: {status.get('phase', 'unknown')}\n"
                text += f"Started: {status.get('started_at', 'N/A')}\n\n"
            else:
                text += "✅ **System Operational**\n\n"
            
            ssh_hosts = get_all_ssh_hosts(enabled_only=False)
            api_hosts = get_all_api_hosts(enabled_only=False)
            
            ssh_online = sum(1 for h in ssh_hosts if h.get('last_status') == 'online' and h['enabled'])
            ssh_offline = sum(1 for h in ssh_hosts if h.get('last_status') != 'online' and h['enabled'])
            ssh_disabled = sum(1 for h in ssh_hosts if not h['enabled'])
            
            api_online = sum(1 for h in api_hosts if h.get('last_status') == 'online' and h['enabled'])
            api_offline = sum(1 for h in api_hosts if h.get('last_status') != 'online' and h['enabled'])
            api_disabled = sum(1 for h in api_hosts if not h['enabled'])
            
            text += f"**SSH Hosts ({len(ssh_hosts)} total)**\n"
            text += f"✅ Online: {ssh_online}\n"
            if ssh_offline > 0:
                text += f"❌ Offline: {ssh_offline}\n"
            if ssh_disabled > 0:
                text += f"⏸️ Disabled: {ssh_disabled}\n"
            text += "\n"
            
            text += f"**API Hosts ({len(api_hosts)} total)**\n"
            text += f"✅ Online: {api_online}\n"
            if api_offline > 0:
                text += f"❌ Offline: {api_offline}\n"
            if api_disabled > 0:
                text += f"⏸️ Disabled: {api_disabled}\n"
            text += "\n"
            
            if ssh_offline > 0 or api_offline > 0:
                text += "⚠️ **Offline Hosts:**\n"
                for h in ssh_hosts:
                    if h['enabled'] and h.get('last_status') != 'online':
                        status_icon = "❌"
                        text += f"{status_icon} `{h['user']}@{h['host']}`\n"
                        text += f"   Status: {h.get('last_status', 'unknown')}\n"
                        if h.get('last_error'):
                            error = h['last_error'][:50]
                            text += f"   Error: {error}\n"
                
                for h in api_hosts:
                    if h['enabled'] and h.get('last_status') != 'online':
                        status_icon = "❌"
                        text += f"{status_icon} `{h['host']}` ({h['api_type']})\n"
                        text += f"   Status: {h.get('last_status', 'unknown')}\n"
                        if h.get('last_error'):
                            error = h['last_error'][:50]
                            text += f"   Error: {error}\n"
            
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, parse_mode="Markdown", reply_markup=reply_markup)
        except Exception as e:
            await query.edit_message_text(f"❌ Error: {str(e)}")
        return
    
    if data == "list_ssh":
        try:
            from database import get_all_ssh_hosts
            hosts = get_all_ssh_hosts(enabled_only=False)
            
            if not hosts:
                text = "📋 SSH Hosts\n\nNo hosts configured."
            else:
                text = "📋 SSH Hosts\n\n"
                for h in hosts[:10]:
                    status_icon = "✅" if h["enabled"] else "⏸️"
                    text += f"{status_icon} `{h['user']}@{h['host']}`\n"
                    text += f"   Status: {h.get('last_status', 'unknown')}\n\n"
                if len(hosts) > 10:
                    text += f"\n...and {len(hosts)-10} more"
            
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, parse_mode="Markdown", reply_markup=reply_markup)
        except Exception as e:
            await query.edit_message_text(f"❌ Error: {str(e)}")
        return
    
    if data == "list_api":
        try:
            from database import get_all_api_hosts
            hosts = get_all_api_hosts(enabled_only=False)
            
            if not hosts:
                text = "📋 API Hosts\n\nNo hosts configured."
            else:
                text = "📋 API Hosts\n\n"
                for h in hosts[:10]:
                    status_icon = "✅" if h["enabled"] else "⏸️"
                    text += f"{status_icon} `{h['host']}` ({h['api_type']})\n"
                    text += f"   Status: {h.get('last_status', 'unknown')}\n\n"
                if len(hosts) > 10:
                    text += f"\n...and {len(hosts)-10} more"
            
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, parse_mode="Markdown", reply_markup=reply_markup)
        except Exception as e:
            await query.edit_message_text(f"❌ Error: {str(e)}")
        return
    
    if data == "add_ssh":
        from auth import get_ssh_public_key
        public_key = get_ssh_public_key()
        
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            "➕ Add SSH Host\n\n"
            "Send details in this format:\n"
            "`ssh:hostname:username::description`\n\n"
            "Example:\n"
            "`ssh:server.local:root::Production server`\n\n"
        )
        
        if public_key:
            text += f"🔑 **Public Key** (add to target `~/.ssh/authorized_keys`):\n`{public_key}`\n\n"
        
        text += (
            "⚠️ **Required on target host:**\n"
            "Add to `/etc/sudoers.d/dms-shutdown`:\n"
            "`username ALL=(ALL) NOPASSWD: /sbin/shutdown`\n\n"
            "Or if no sudoers support, configure SSH as root user.\n\n"
            "TOTP code will be required after connection test."
        )
        
        await query.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=reply_markup
        )
        return
    
    if data == "add_api":
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "➕ Add API Host\n\n"
            "Send details in this format:\n"
            "`api:type:hostname:api_key:api_endpoint:description`\n\n"
            "If any field contains colons, you can use `|` as delimiter:\n"
            "`api|type|hostname|api_key|api_endpoint|description`\n\n"
            "Example:\n"
            "`api:vcenter:vcenter.local:admin@vsphere.local:password:vCenter`\n\n"
            "⚠️ TOTP code will be required after connection test.",
            parse_mode="Markdown",
            reply_markup=reply_markup
        )
        return
    
    if data == "remove_ssh":
        try:
            from database import get_all_ssh_hosts
            hosts = get_all_ssh_hosts(enabled_only=False)
            
            if not hosts:
                from telegram import InlineKeyboardButton, InlineKeyboardMarkup
                keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text("📋 No SSH hosts to remove.", reply_markup=reply_markup)
                return
            
            text = "🗑️ Remove SSH Host\n\nSend the host to remove:\n`host:user`\n\n"
            for h in hosts[:10]:
                text += f"• `{h['host']}:{h['user']}`\n"
            if len(hosts) > 10:
                text += f"\n...and {len(hosts)-10} more"
            
            text += "\n⚠️ TOTP code will be required."
            
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, parse_mode="Markdown", reply_markup=reply_markup)
        except Exception as e:
            await query.edit_message_text(f"❌ Error: {str(e)}")
        return
    
    if data == "remove_api":
        try:
            from database import get_all_api_hosts
            hosts = get_all_api_hosts(enabled_only=False)
            
            if not hosts:
                from telegram import InlineKeyboardButton, InlineKeyboardMarkup
                keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text("📋 No API hosts to remove.", reply_markup=reply_markup)
                return
            
            text = "🗑️ Remove API Host\n\nSend the hostname to remove:\n\n"
            for h in hosts[:10]:
                text += f"• `{h['host']}`\n"
            if len(hosts) > 10:
                text += f"\n...and {len(hosts)-10} more"
            
            text += "\n⚠️ TOTP code will be required."
            
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, parse_mode="Markdown", reply_markup=reply_markup)
        except Exception as e:
            await query.edit_message_text(f"❌ Error: {str(e)}")
        return
    
    if data == "shutdown":
        _pending_operations[user_id] = {"operation": "shutdown", "state": "awaiting_otp"}
        
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "🚨 EMERGENCY SHUTDOWN (ALL HOSTS)\n\n"
            "⚠️ This will shutdown ALL infrastructure!\n\n"
            "Send your TOTP code to confirm.",
            reply_markup=reply_markup
        )
        return
    
    if data == "selective_shutdown":
        try:
            from database import get_all_ssh_hosts, get_all_api_hosts
            ssh_hosts = get_all_ssh_hosts(enabled_only=True)
            api_hosts = get_all_api_hosts(enabled_only=True)
            
            if not ssh_hosts and not api_hosts:
                from telegram import InlineKeyboardButton, InlineKeyboardMarkup
                keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(
                    "📋 No hosts available for shutdown.",
                    reply_markup=reply_markup
                )
                return
            
            text = "⚡ **Selective Shutdown**\n\nChoose hosts to shutdown:\n\n"
            text += "Send host identifiers (comma-separated):\n\n"
            
            host_list = []
            if ssh_hosts:
                text += "**SSH Hosts:**\n"
                for idx, h in enumerate(ssh_hosts, 1):
                    host_id = f"ssh:{h['host']}:{h['user']}"
                    host_list.append(host_id)
                    text += f"{idx}. `{h['user']}@{h['host']}`\n"
                text += "\n"
            
            if api_hosts:
                text += "**API Hosts:**\n"
                offset = len(ssh_hosts)
                for idx, h in enumerate(api_hosts, offset + 1):
                    host_id = f"api:{h['host']}:{h['api_type']}"
                    host_list.append(host_id)
                    text += f"{idx}. `{h['host']}` ({h['api_type']})\n"
                text += "\n"
            
            text += "\nExamples:\n"
            text += "`1,3,5` to shutdown hosts 1, 3, and 5\n"
            text += "`1-4` to shutdown hosts 1 through 4\n"
            text += "`all` to shutdown all hosts\n"
            
            _pending_operations[user_id] = {
                "operation": "selective_shutdown",
                "state": "awaiting_selection",
                "host_list": host_list
            }
            
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, parse_mode="Markdown", reply_markup=reply_markup)
        except Exception as e:
            await query.edit_message_text(f"❌ Error: {str(e)}")
        return


async def _message_handler(update: Any, context: Any):
    """Handle text messages."""
    user_id = update.effective_user.id
    
    if str(user_id) != TELEGRAM_CHAT_ID:
        return
    
    message_text = update.message.text.strip()
    
    if user_id in _pending_auth:
        state = _pending_auth[user_id]
        
        if state == "awaiting_token":
            try:
                from auth import verify_static_token_value
                
                if verify_static_token_value(message_text):
                    _pending_auth[user_id] = "awaiting_otp"
                    try:
                        await update.message.delete()
                    except:
                        pass
                    await update.message.reply_text(
                        "✅ Token verified\n\n"
                        "🔐 Authentication (Step 2/2)\n\n"
                        "Send your TOTP code."
                    )
                else:
                    await update.message.reply_text("❌ Invalid token")
                    del _pending_auth[user_id]
                    logger.warning(f"Invalid token from user {user_id}")
            except Exception as e:
                await update.message.reply_text(f"❌ Error: {str(e)}")
                del _pending_auth[user_id]
            return
        
        if state == "awaiting_otp":
            try:
                from auth import verify_totp
                from database import add_telegram_session
                
                if verify_totp(message_text):
                    add_telegram_session(user_id)
                    del _pending_auth[user_id]
                    
                    try:
                        await update.message.delete()
                    except:
                        pass
                    
                    await update.message.reply_text("✅ Authenticated (expires in 24h)")
                    logger.info(f"User {user_id} authenticated")
                    
                    await _show_main_menu(update, True)
                else:
                    await update.message.reply_text("❌ Invalid TOTP code")
                    del _pending_auth[user_id]
                    logger.warning(f"Invalid TOTP from user {user_id}")
            except Exception as e:
                await update.message.reply_text(f"❌ Error: {str(e)}")
                del _pending_auth[user_id]
        return
    
    if user_id in _pending_operations:
        op = _pending_operations[user_id]
        
        if op.get("operation") == "selective_shutdown" and op.get("state") == "awaiting_selection":
            try:
                # Parse selection
                selection = message_text.strip().lower()
                host_list = op.get("host_list", [])
                
                if not host_list:
                    await update.message.reply_text("❌ No hosts available")
                    del _pending_operations[user_id]
                    return
                
                selected_indices = []
                if selection == "all":
                    selected_indices = list(range(len(host_list)))
                else:
                    # Parse comma-separated and ranges
                    for part in selection.split(","):
                        part = part.strip()
                        if "-" in part:
                            try:
                                start, end = part.split("-")
                                start_idx = int(start.strip()) - 1
                                end_idx = int(end.strip()) - 1
                                selected_indices.extend(range(start_idx, end_idx + 1))
                            except:
                                pass
                        else:
                            try:
                                selected_indices.append(int(part) - 1)
                            except:
                                pass
                
                # Validate and collect hosts
                selected_hosts = []
                for idx in selected_indices:
                    if 0 <= idx < len(host_list):
                        selected_hosts.append(host_list[idx])
                
                if not selected_hosts:
                    await update.message.reply_text("❌ Invalid selection. Try again or send /start to cancel.")
                    return
                
                # Show confirmation
                try:
                    await update.message.delete()
                except:
                    pass
                
                text = f"⚠️ **Confirm Selective Shutdown**\n\n"
                text += f"**Selected {len(selected_hosts)} host(s):**\n"
                for host_id in selected_hosts:
                    parts = host_id.split(":", 2)
                    if parts[0] == "ssh":
                        text += f"• SSH: `{parts[2]}@{parts[1]}`\n"
                    else:
                        text += f"• API: `{parts[1]}` ({parts[2]})\n"
                text += f"\nSend your TOTP code to confirm shutdown."
                
                _pending_operations[user_id] = {
                    "operation": "selective_shutdown",
                    "state": "awaiting_otp",
                    "selected_hosts": selected_hosts
                }
                
                await update.message.reply_text(text, parse_mode="Markdown")
            except Exception as e:
                await update.message.reply_text(f"❌ Error: {str(e)}")
                del _pending_operations[user_id]
            return
        
        if op["state"] == "awaiting_otp":
            try:
                from auth import verify_totp
                
                if verify_totp(message_text):
                    operation = op["operation"]
                    data = op.get("data", {})
                    del _pending_operations[user_id]
                    
                    try:
                        await update.message.delete()
                    except:
                        pass
                    
                    if operation == "shutdown":
                        status_msg = await update.message.reply_text(
                            "✅ TOTP verified\n\n"
                            "🚨 INITIATING EMERGENCY SHUTDOWN..."
                        )
                        
                        from dms_logic import initiate_hard_poweroff
                        result = initiate_hard_poweroff()
                        
                        # Build detailed results message
                        text = "🚨 **EMERGENCY SHUTDOWN EXECUTED**\n\n"
                        
                        # Show results for each phase
                        results = result.get("results", {})
                        total_hosts = 0
                        success_count = 0
                        
                        for phase, hosts in results.items():
                            if hosts:
                                text += f"**{phase.upper()}:**\n"
                                for h in hosts:
                                    total_hosts += 1
                                    host_name = h.get("host", "unknown")
                                    status = h.get("status", "unknown")
                                    details = h.get("details", "")
                                    
                                    if status in ["shutdown_initiated", "executed"]:
                                        icon = "✅"
                                        success_count += 1
                                    elif status == "timeout":
                                        icon = "⏱️"
                                        success_count += 1  # timeout often means it worked
                                    else:
                                        icon = "❌"
                                    
                                    text += f"{icon} `{host_name}` - {status}\n"
                                    if details and status not in ["shutdown_initiated", "executed"]:
                                        text += f"   _{details[:50]}_\n"
                                text += "\n"
                        
                        text += f"**Summary:** {success_count}/{total_hosts} hosts executed\n"
                        
                        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
                        keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="back")]]
                        reply_markup = InlineKeyboardMarkup(keyboard)
                        
                        try:
                            await status_msg.edit_text(text, parse_mode="Markdown", reply_markup=reply_markup)
                        except:
                            await update.message.reply_text(text, parse_mode="Markdown", reply_markup=reply_markup)
                        
                        logger.critical(f"Shutdown triggered via Telegram by user {user_id}")
                    
                    elif operation == "selective_shutdown":
                        selected_hosts = data.get("selected_hosts", [])
                        if not selected_hosts:
                            await update.message.reply_text("❌ No hosts selected")
                            return
                        
                        status_msg = await update.message.reply_text(
                            f"✅ TOTP verified\n\n"
                            f"⚡ INITIATING SELECTIVE SHUTDOWN ({len(selected_hosts)} host(s))..."
                        )
                        
                        # Execute selective shutdown
                        from dms_logic import execute_shutdown_phase
                        from database import get_all_ssh_hosts, get_all_api_hosts
                        
                        results = {"ssh": [], "api": []}
                        
                        for host_id in selected_hosts:
                            parts = host_id.split(":", 2)
                            if parts[0] == "ssh":
                                # Find SSH host
                                ssh_hosts = get_all_ssh_hosts(enabled_only=True)
                                target = next((h for h in ssh_hosts if h['host'] == parts[1] and h['user'] == parts[2]), None)
                                if target:
                                    result = execute_shutdown_phase([target], "ssh", "SSH")
                                    results["ssh"].extend(result)
                            elif parts[0] == "api":
                                # Find API host
                                api_hosts = get_all_api_hosts(enabled_only=True)
                                target = next((h for h in api_hosts if h['host'] == parts[1] and h['api_type'] == parts[2]), None)
                                if target:
                                    result = execute_shutdown_phase([target], parts[2], parts[2].upper())
                                    results["api"].extend(result)
                        
                        # Build results message
                        text = f"⚡ **SELECTIVE SHUTDOWN EXECUTED**\n\n"
                        total_hosts = 0
                        success_count = 0
                        
                        for phase, hosts in results.items():
                            if hosts:
                                text += f"**{phase.upper()}:**\n"
                                for h in hosts:
                                    total_hosts += 1
                                    host_name = h.get("host", "unknown")
                                    status = h.get("status", "unknown")
                                    details = h.get("details", "")
                                    
                                    if status in ["shutdown_initiated", "executed"]:
                                        icon = "✅"
                                        success_count += 1
                                    elif status == "timeout":
                                        icon = "⏱️"
                                        success_count += 1
                                    else:
                                        icon = "❌"
                                    
                                    text += f"{icon} `{host_name}` - {status}\n"
                                    if details and status not in ["shutdown_initiated", "executed"]:
                                        text += f"   _{details[:50]}_\n"
                                text += "\n"
                        
                        text += f"**Summary:** {success_count}/{total_hosts} hosts executed\n"
                        
                        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
                        keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="back")]]
                        reply_markup = InlineKeyboardMarkup(keyboard)
                        
                        try:
                            await status_msg.edit_text(text, parse_mode="Markdown", reply_markup=reply_markup)
                        except:
                            await update.message.reply_text(text, parse_mode="Markdown", reply_markup=reply_markup)
                        
                        logger.critical(f"Selective shutdown triggered via Telegram by user {user_id}: {len(selected_hosts)} hosts")
                    
                    elif operation == "add_ssh":
                        from database import add_ssh_host, get_all_ssh_hosts
                        if add_ssh_host(data["host"], data["user"], data["description"]):
                            # Test immediately after adding
                            from dms_logic import monitor_ssh_host
                            hosts = get_all_ssh_hosts(enabled_only=False)
                            host_data = next((h for h in hosts if h['host'] == data['host'] and h['user'] == data['user']), None)
                            if host_data:
                                monitor_ssh_host(host_data)
                            
                            await update.message.reply_text(f"✅ SSH host added: {data['user']}@{data['host']}")
                            logger.info(f"SSH host {data['user']}@{data['host']} added via Telegram by user {user_id}")
                            await _show_main_menu(update, True)
                        else:
                            await update.message.reply_text("❌ Failed to add host (may already exist)")
                    
                    elif operation == "add_api":
                        from database import add_api_host, get_all_api_hosts
                        if add_api_host(data["host"], data["api_type"], data["api_key"], data["api_endpoint"], data["description"]):
                            # Test immediately after adding
                            from dms_logic import monitor_api_host
                            hosts = get_all_api_hosts(enabled_only=False)
                            host_data = next((h for h in hosts if h['host'] == data['host']), None)
                            if host_data:
                                monitor_api_host(host_data)
                            
                            await update.message.reply_text(f"✅ API host added: {data['host']} ({data['api_type']})")
                            logger.info(f"API host {data['host']} ({data['api_type']}) added via Telegram by user {user_id}")
                            await _show_main_menu(update, True)
                        else:
                            await update.message.reply_text("❌ Failed to add host (may already exist)")
                    
                    elif operation == "remove_ssh":
                        from database import delete_ssh_host
                        if delete_ssh_host(data["host"], data["user"]):
                            await update.message.reply_text(f"✅ SSH host removed: {data['user']}@{data['host']}")
                            logger.info(f"SSH host {data['user']}@{data['host']} removed via Telegram by user {user_id}")
                            await _show_main_menu(update, True)
                        else:
                            await update.message.reply_text("❌ Host not found")
                    
                    elif operation == "remove_api":
                        from database import delete_api_host
                        if delete_api_host(data["host"]):
                            await update.message.reply_text(f"✅ API host removed: {data['host']}")
                            logger.info(f"API host {data['host']} removed via Telegram by user {user_id}")
                            await _show_main_menu(update, True)
                        else:
                            await update.message.reply_text("❌ Host not found")
                else:
                    await update.message.reply_text("❌ Invalid TOTP code")
                    del _pending_operations[user_id]
                    logger.warning(f"Invalid TOTP from user {user_id}")
            except Exception as e:
                await update.message.reply_text(f"❌ Error: {str(e)}")
                del _pending_operations[user_id]
        return
    
    from database import is_telegram_session_valid
    if not is_telegram_session_valid(user_id):
        return
    
    if message_text.startswith("ssh:"):
        try:
            parts = message_text.split(":", 5)
            if len(parts) < 3:
                await update.message.reply_text("❌ Invalid format. Need: ssh:host:user::description")
                return
            
            _, host, user = parts[:3]
            # command field deprecated; allow empty segment
            description = parts[4] if len(parts) > 4 else (parts[3] if len(parts) > 3 else "")
            
            from dms_logic import test_ssh_connection
            
            test = test_ssh_connection(host, user)
            
            if not test["success"]:
                await update.message.reply_text(f"❌ Connection test failed: {test['error']}")
                return
            
            try:
                await update.message.delete()
            except:
                pass
            
            _pending_operations[user_id] = {
                "operation": "add_ssh",
                "state": "awaiting_otp",
                "data": {"host": host, "user": user, "description": description}
            }
            await update.message.reply_text(
                f"✅ Connection test successful\n\n"
                f"Send your TOTP code to confirm adding:\n`{user}@{host}`",
                parse_mode="Markdown"
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")
        return
    
    if message_text.startswith(("api:", "api|")):
        try:
            delimiter = ":" if message_text.startswith("api:") else "|"
            parts = message_text.split(delimiter, 6)
            if len(parts) < 4:
                await update.message.reply_text("❌ Invalid format. Need: api:type:host:api_key:api_endpoint:description (or use | as delimiter)")
                return
            
            _, api_type, host, api_key = parts[:4]
            api_endpoint = parts[4] if len(parts) > 4 else ""
            description = parts[5] if len(parts) > 5 else ""

            # Trim accidental whitespace/newlines from individual fields to avoid login failures
            api_type = api_type.strip()
            host = host.strip()
            api_key = api_key.strip()
            api_endpoint = api_endpoint.strip()
            description = description.strip()
            
            from dms_logic import test_api_connection
            from plugins import list_plugins
            
            if api_type not in list_plugins():
                await update.message.reply_text(f"❌ Unknown type: {api_type}")
                return
            
            test = test_api_connection(host, api_type, api_key, api_endpoint)
            
            if not test["success"]:
                await update.message.reply_text(f"❌ Connection test failed: {test['error']}")
                return
            
            try:
                await update.message.delete()
            except:
                pass
            
            _pending_operations[user_id] = {
                "operation": "add_api",
                "state": "awaiting_otp",
                "data": {"host": host, "api_type": api_type, "api_key": api_key, "api_endpoint": api_endpoint, "description": description}
            }
            await update.message.reply_text(
                f"✅ Connection test successful\n\n"
                f"Send your TOTP code to confirm adding:\n`{host}` ({api_type})",
                parse_mode="Markdown"
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {str(e)}")
        return
    
    # Remove host (SSH if has colon, API otherwise)
    if ":" in message_text and not message_text.startswith(("ssh:", "api:")):
        parts = message_text.split(":")
        if len(parts) == 2:
            try:
                await update.message.delete()
            except:
                pass
            
            _pending_operations[user_id] = {
                "operation": "remove_ssh",
                "state": "awaiting_otp",
                "data": {"host": parts[0], "user": parts[1]}
            }
            await update.message.reply_text(
                f"⚠️ Remove SSH host:\n`{parts[1]}@{parts[0]}`\n\n"
                f"Send your TOTP code to confirm.",
                parse_mode="Markdown"
            )
            return
    
    if not message_text.startswith(("ssh:", "api:")) and ":" not in message_text and len(message_text) > 3:
        try:
            await update.message.delete()
        except:
            pass
        
        _pending_operations[user_id] = {
            "operation": "remove_api",
            "state": "awaiting_otp",
            "data": {"host": message_text}
        }
        await update.message.reply_text(
            f"⚠️ Remove API host:\n`{message_text}`\n\n"
            f"Send your TOTP code to confirm.",
            parse_mode="Markdown"
        )
        return


def _run_bot_polling_sync():
    """Run bot polling in background thread using async approach."""
    import asyncio
    
    async def _async_main():
        from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters
        from telegram import Update

        app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

        app.add_handler(CommandHandler("start", _cmd_start))
        app.add_handler(CommandHandler("status", _cmd_status))
        app.add_handler(CommandHandler("logout", _cmd_logout))
        app.add_handler(CallbackQueryHandler(_button_callback))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _message_handler))

        logger.info("Telegram bot handlers registered")
        logger.info("Bot ready for commands")

        # Initialize and start polling manually for background thread compatibility
        async with app:
            await app.start()
            logger.info("Telegram bot started successfully")
            # Start polling without signals (safe for background threads)
            if app.updater:
                await app.updater.start_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
            # Keep running
            await asyncio.Event().wait()
    
    try:
        asyncio.run(_async_main())
    except Exception as e:
        logger.error(f"Bot polling error: {e}")


def start_bot():
    """Start Telegram bot in background task."""
    if not _telegram_enabled:
        logger.info("Telegram bot disabled (not configured)")
        return
    
    try:
        logger.info("Starting Telegram bot polling...")
        import threading

        bot_thread = threading.Thread(target=_run_bot_polling_sync, daemon=True)
        bot_thread.start()
        logger.info("Telegram bot thread started")
        
    except Exception as e:
        logger.error(f"Failed to start Telegram bot: {e}")
        logger.error("Check token and network connectivity")


def notify_new_ip(ip: str, endpoint: str):
    """Notify about new IP access."""
    message = f"New IP detected\nIP: `{ip}`\nEndpoint: `{endpoint}`"
    _send_message(message, critical=True)


def notify_shutdown():
    """Notify about shutdown initiation."""
    message = "Shutdown sequence initiated"
    _send_message(message, critical=True)


def notify_host_added(host: str, host_type: str):
    """Notify about host addition."""
    message = f"Host added\nType: {host_type}\nHost: `{host}`"
    _send_message(message, critical=False)


def notify_host_removed(host: str):
    """Notify about host removal."""
    message = f"Host removed\nHost: `{host}`"
    _send_message(message, critical=False)
