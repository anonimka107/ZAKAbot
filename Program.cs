using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using IOFile = System.IO.File;
using Telegram.Bot;
using Telegram.Bot.Exceptions;
using Telegram.Bot.Polling;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;
using Telegram.Bot.Types.ReplyMarkups;

namespace ZakaBot
{
    internal static class Program
    {
        private static async Task Main()
        {
            var config = BotConfig.Load();
            if (string.IsNullOrWhiteSpace(config.TelegramBotToken) ||
                config.TelegramBotToken == "PASTE_YOUR_TOKEN_HERE")
            {
                Console.WriteLine("Ошибка: укажи TELEGRAM_BOT_TOKEN или TelegramBotToken в appsettings.local.json");
                return;
            }

            var app = new ZakaBotApp(config);
            await app.RunAsync();
        }
    }

    internal sealed class ZakaBotApp
    {
        private const string AdminUsername = "edelweiss_kk";
        private const string DarlingUsername = "N7WAHaRBRiiNGeR";
        private const string StrangerText = "ты не зайка.";
        private const string DarlingStartText = "ооооой зайка привет, я вот ботика тебе сделал <3";
        private const string AdminStartText = "салам владос, делаем ";
        private const string DarlingForwardAckText = "отправил твое сообщение админу, вам ответит первый свободный владик <3";
        private const string DarlingOutgoingDisabledText = "отправка сообщений зайке выключена";
        private static readonly TimeSpan Daily143Time = new TimeSpan(1, 43, 0);

        private readonly TelegramBotClient _bot;
        private readonly JsonStorage _storage;
        private readonly Random _random = new Random();
        private readonly TimeZoneInfo _moscowTimeZone;
        private readonly DateTime _startedAtMoscow;
        private readonly object _sessionLock = new object();

        private BotState _state = new BotState();
        private MessageBank _messages = new MessageBank();
        private AdminSession? _adminSession;
        private CancellationTokenSource? _darlingAckCts;
        private bool _manualReplySinceLastDarlingMessage;

        public ZakaBotApp(BotConfig config)
        {
            _bot = new TelegramBotClient(config.TelegramBotToken);
            _storage = new JsonStorage(config.BotDataDir);
            _moscowTimeZone = TimeZoneHelper.GetMoscowTimeZone();
            _startedAtMoscow = NowMoscow();
        }

        public async Task RunAsync()
        {
            Directory.CreateDirectory(_storage.DataDir);
            _messages = await _storage.LoadMessagesAsync();
            _state = await _storage.LoadStateAsync();
            CleanupExpiredOneTimeOverrides();
            await SaveStateAsync();

            Console.WriteLine("Бот запускается...");
            Console.WriteLine("Папка данных: " + _storage.DataDir);

            using (var cts = new CancellationTokenSource())
            {
                Console.CancelKeyPress += (sender, args) =>
                {
                    args.Cancel = true;
                    cts.Cancel();
                };

                var me = await _bot.GetMeAsync(cts.Token);
                Console.WriteLine("Бот запущен: @" + me.Username);

                if (_state.AdminUserId.HasValue)
                {
                    await SafeSendTextAsync(_state.AdminUserId.Value, "бот запущен", AdminReplyKeyboard(), cts.Token);
                }

                var receiverOptions = new ReceiverOptions
                {
                    AllowedUpdates = new[]
                    {
                        UpdateType.Message,
                        UpdateType.CallbackQuery
                    }
                };

                _bot.StartReceiving(
                    HandleUpdateAsync,
                    HandlePollingErrorAsync,
                    receiverOptions,
                    cts.Token);

                var schedulerTask = RunSchedulerAsync(cts.Token);

                Console.WriteLine("Нажми Ctrl+C для остановки.");
                try
                {
                    await Task.Delay(Timeout.Infinite, cts.Token);
                }
                catch (TaskCanceledException)
                {
                }

                await schedulerTask;
            }
        }

        private async Task HandleUpdateAsync(ITelegramBotClient botClient, Update update, CancellationToken ct)
        {
            try
            {
                if (update.Type == UpdateType.CallbackQuery && update.CallbackQuery != null)
                {
                    await HandleCallbackAsync(update.CallbackQuery, ct);
                    return;
                }

                if (update.Type == UpdateType.Message && update.Message != null)
                {
                    await HandleMessageAsync(update.Message, ct);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Ошибка обработки update: " + ex);
                if (_state.AdminUserId.HasValue)
                {
                    await SafeSendTextAsync(_state.AdminUserId.Value, "ошибка обработки сообщения: " + ex.Message, ct);
                }
            }
        }

        private Task HandlePollingErrorAsync(ITelegramBotClient botClient, Exception exception, CancellationToken ct)
        {
            var message = exception is ApiRequestException apiException
                ? "Telegram API ошибка: " + apiException.ErrorCode + " " + apiException.Message
                : "Polling ошибка: " + exception;

            Console.WriteLine(message);
            return Task.CompletedTask;
        }

        private async Task HandleMessageAsync(Message message, CancellationToken ct)
        {
            if (message.From == null)
            {
                return;
            }

            var role = await ResolveRoleAndRememberAsync(message.From, ct);
            if (role == UserRole.Stranger)
            {
                await SafeSendTextAsync(message.Chat.Id, StrangerText, ct);
                return;
            }

            var text = message.Text ?? string.Empty;

            if (role == UserRole.Admin)
            {
                if (text == "/start")
                {
                    await SafeSendTextAsync(message.Chat.Id, AdminStartText, AdminReplyKeyboard(), ct);
                    await ShowAdminMenuAsync(message.Chat.Id, ct);
                    return;
                }

                if (text == "/admin" || text == "Админка")
                {
                    ClearAdminSession();
                    await ShowAdminMenuAsync(message.Chat.Id, ct);
                    return;
                }

                if (text == "/cancel")
                {
                    ClearAdminSession();
                    await SafeSendTextAsync(message.Chat.Id, "отменил", AdminReplyKeyboard(), ct);
                    await ShowAdminMenuAsync(message.Chat.Id, ct);
                    return;
                }

                await HandleAdminTextAsync(message, ct);
                return;
            }

            if (text == "/start")
            {
                if (!_state.DarlingOutgoingEnabled)
                {
                    return;
                }

                await SafeSendTextAsync(message.Chat.Id, DarlingStartText, ct);
                return;
            }

            await ForwardDarlingMessageToAdminAsync(message, ct);
        }

        private async Task HandleCallbackAsync(CallbackQuery callback, CancellationToken ct)
        {
            if (callback.From == null || callback.Message == null)
            {
                return;
            }

            var role = await ResolveRoleAndRememberAsync(callback.From, ct);
            if (role != UserRole.Admin)
            {
                await _bot.AnswerCallbackQueryAsync(callback.Id, StrangerText, cancellationToken: ct);
                if (callback.Message.Chat != null)
                {
                    await SafeSendTextAsync(callback.Message.Chat.Id, StrangerText, ct);
                }
                return;
            }

            await _bot.AnswerCallbackQueryAsync(callback.Id, cancellationToken: ct);

            var chatId = callback.Message.Chat.Id;
            var messageId = callback.Message.MessageId;
            var data = callback.Data ?? string.Empty;

            if (data != "confirm:yes" &&
                data != "confirm:no" &&
                !data.StartsWith("refresh:", StringComparison.Ordinal))
            {
                ClearAdminSession();
            }

            if (data == "flow:cancel")
            {
                await ShowAdminMenuAsync(chatId, ct, messageId);
                return;
            }

            if (data == "menu:main")
            {
                ClearAdminSession();
                await ShowAdminMenuAsync(chatId, ct, messageId);
                return;
            }

            if (data == "menu:status")
            {
                await ShowStatusAsync(chatId, ct, messageId);
                return;
            }

            if (data == "menu:notifications")
            {
                _state.AdminNotificationsEnabled = !_state.AdminNotificationsEnabled;
                await SaveStateAsync();
                Console.WriteLine("Уведомления админу: " + (_state.AdminNotificationsEnabled ? "включены" : "выключены"));
                await ShowAdminMenuAsync(chatId, ct, messageId, _state.AdminNotificationsEnabled ? "уведомления включены" : "уведомления выключены");
                return;
            }

            if (data == "menu:daily143")
            {
                _state.Daily143Enabled = !_state.Daily143Enabled;
                await SaveStateAsync();
                await ShowAdminMenuAsync(chatId, ct, messageId, _state.Daily143Enabled ? "1:43 включено" : "1:43 выключено");
                return;
            }

            if (data == "menu:darling_outgoing")
            {
                _state.DarlingOutgoingEnabled = !_state.DarlingOutgoingEnabled;
                await SaveStateAsync();
                await ShowAdminMenuAsync(chatId, ct, messageId, _state.DarlingOutgoingEnabled ? "отправка сообщений зайке включена" : "отправка сообщений зайке выключена");
                return;
            }

            if (data == "menu:send_now")
            {
                await StartInputSessionAsync(chatId, messageId, new AdminSession(PendingAction.SendNow), "напиши текст, который отправить зайке", ct);
                return;
            }

            if (data == "menu:send_file")
            {
                await StartInputSessionAsync(chatId, messageId, new AdminSession(PendingAction.SendFile), "пришли файл, песню, фото, видео, голосовое или стикер", ct);
                return;
            }

            if (data == "reply:darling")
            {
                await StartInputSessionAsync(chatId, messageId, new AdminSession(PendingAction.ReplyToDarling), "напиши ответ зайке", ct);
                return;
            }

            if (data == "menu:bank")
            {
                await ShowBankMenuAsync(chatId, ct, messageId);
                return;
            }

            if (data == "menu:refresh_bank")
            {
                SetAdminSession(new AdminSession(PendingAction.ConfirmRefreshAllBank));
                await EditOrSendTextAsync(
                    chatId,
                    messageId,
                    "начать обновление всего банка? текущие сообщения сохранятся до финального подтверждения",
                    ConfirmKeyboard(),
                    ct);
                return;
            }

            if (data.StartsWith("refresh:", StringComparison.Ordinal))
            {
                await HandleRefreshBankCallbackAsync(chatId, messageId, data, ct);
                return;
            }

            if (data.StartsWith("view:", StringComparison.Ordinal))
            {
                await HandleViewBankCallbackAsync(chatId, messageId, data, ct);
                return;
            }

            if (data.StartsWith("bank:", StringComparison.Ordinal))
            {
                await HandleBankActionCallbackAsync(chatId, messageId, data, ct);
                return;
            }

            if (data == "menu:morning")
            {
                await ShowReminderMenuAsync(chatId, ReminderKind.Morning, ct, messageId);
                return;
            }

            if (data == "menu:night")
            {
                await ShowReminderMenuAsync(chatId, ReminderKind.Night, ct, messageId);
                return;
            }

            if (data.StartsWith("rem:", StringComparison.Ordinal))
            {
                await HandleReminderCallbackAsync(chatId, messageId, data, ct);
                return;
            }

            if (data == "confirm:yes" || data == "confirm:no")
            {
                await HandleConfirmationAsync(chatId, messageId, data == "confirm:yes", ct);
                return;
            }
        }

        private async Task HandleAdminTextAsync(Message message, CancellationToken ct)
        {
            var session = GetActiveAdminSession();
            if (session == null)
            {
                return;
            }

            var chatId = message.Chat.Id;
            if (session.Action == PendingAction.SendFile)
            {
                await HandleAdminFileInputAsync(chatId, session, message, ct);
                return;
            }

            var text = message.Text;
            if (string.IsNullOrEmpty(text))
            {
                await UpdateSessionMessageAsync(chatId, session, "нужен текст\n\nпопробуй еще раз", CancelKeyboard(), ct);
                return;
            }

            if (text.Length > 4096)
            {
                await UpdateSessionMessageAsync(chatId, session, "текст слишком длинный, максимум 4096 символов\n\nпопробуй еще раз", CancelKeyboard(), ct);
                return;
            }

            session.Touch();

            switch (session.Action)
            {
                case PendingAction.AddMessage:
                    AddMessage(session.Bank, text);
                    await SaveMessagesAsync();
                    await SaveStateAsync();
                    Console.WriteLine("Добавлено сообщение в банк: " + GetBankTitle(session.Bank));
                    ClearAdminSession();
                    await ShowBankMenuAsync(chatId, ct, session.PromptMessageId, "добавил");
                    break;

                case PendingAction.DeleteMessageNumber:
                    await HandleDeleteNumberAsync(chatId, session, text, ct);
                    break;

                case PendingAction.EditMessageNumber:
                    await HandleEditNumberAsync(chatId, session, text, ct);
                    break;

                case PendingAction.EditMessageText:
                    session.NewText = text;
                    session.Action = PendingAction.ConfirmEditMessage;
                    await UpdateSessionMessageAsync(chatId, session, "сохранить новый текст?\n\n" + text, ConfirmKeyboard(), ct);
                    break;

                case PendingAction.SendNow:
                    session.NewText = text;
                    session.Action = PendingAction.ConfirmSendNow;
                    await UpdateSessionMessageAsync(chatId, session, "отправить зайке?\n\n" + text, ConfirmKeyboard(), ct);
                    break;

                case PendingAction.ReplyToDarling:
                    session.NewText = text;
                    session.Action = PendingAction.ConfirmReplyToDarling;
                    await UpdateSessionMessageAsync(chatId, session, "отправить зайке?\n\n" + text, ConfirmKeyboard(), ct);
                    break;

                case PendingAction.ChangeTimeInput:
                    await HandleChangeTimeInputAsync(chatId, session, text, ct);
                    break;

                case PendingAction.PostponeInput:
                    await HandlePostponeInputAsync(chatId, session, text, ct);
                    break;

                case PendingAction.CollectRefreshMorning:
                    session.DraftMorning.Add(text);
                    SetAdminSession(session);
                    break;

                case PendingAction.CollectRefreshNight:
                    session.DraftNight.Add(text);
                    SetAdminSession(session);
                    break;

                case PendingAction.RefreshAddText:
                    GetDraftBank(session, session.Bank).Add(text);
                    await ShowRefreshBankReviewAsync(chatId, ct, session.PromptMessageId, "добавил", session);
                    break;

                case PendingAction.RefreshDeleteNumber:
                    await HandleRefreshDeleteNumberAsync(chatId, session, text, ct);
                    break;

                case PendingAction.RefreshEditNumber:
                    await HandleRefreshEditNumberAsync(chatId, session, text, ct);
                    break;

                case PendingAction.RefreshEditText:
                    GetDraftBank(session, session.Bank)[session.MessageIndex.GetValueOrDefault()] = text;
                    await ShowRefreshBankReviewAsync(chatId, ct, session.PromptMessageId, "сохранил", session);
                    break;
            }
        }

        private async Task HandleDeleteNumberAsync(long chatId, AdminSession session, string text, CancellationToken ct)
        {
            int number;
            if (!int.TryParse(text.Trim(), out number) || number < 1 || number > GetBank(session.Bank).Count)
            {
                await UpdateSessionMessageAsync(chatId, session, "нет такого номера, введи заново", CancelKeyboard(), ct);
                return;
            }

            session.MessageIndex = number - 1;
            session.Action = PendingAction.ConfirmDeleteMessage;

            await UpdateSessionMessageAsync(chatId, session, "точно удалить?\n\n" + GetBank(session.Bank)[number - 1], ConfirmKeyboard(), ct);
        }

        private async Task HandleAdminFileInputAsync(long chatId, AdminSession session, Message message, CancellationToken ct)
        {
            if (!HasSendableFileContent(message))
            {
                await UpdateSessionMessageAsync(chatId, session, "это не файл\n\nпришли песню, документ, фото, видео, голосовое или стикер", CancelKeyboard(), ct);
                return;
            }

            session.SourceChatId = chatId;
            session.SourceMessageId = message.MessageId;
            session.Action = PendingAction.ConfirmSendFile;
            await UpdateSessionMessageAsync(chatId, session, "отправить зайке этот файл?", ConfirmKeyboard(), ct);
        }

        private async Task HandleEditNumberAsync(long chatId, AdminSession session, string text, CancellationToken ct)
        {
            int number;
            if (!int.TryParse(text.Trim(), out number) || number < 1 || number > GetBank(session.Bank).Count)
            {
                await UpdateSessionMessageAsync(chatId, session, "нет такого номера, введи заново", CancelKeyboard(), ct);
                return;
            }

            session.MessageIndex = number - 1;
            session.Action = PendingAction.EditMessageText;

            await UpdateSessionMessageAsync(chatId, session, "старый текст:\n\n" + GetBank(session.Bank)[number - 1] + "\n\nотправь новый текст", CancelKeyboard(), ct);
        }

        private async Task HandleChangeTimeInputAsync(long chatId, AdminSession session, string text, CancellationToken ct)
        {
            TimeSpan time;
            if (!TryParseTime(text, out time))
            {
                await UpdateSessionMessageAsync(chatId, session, "не понял время, введи например 9:45 или 09:45", CancelKeyboard(), ct);
                return;
            }

            session.NewDailyTime = time;
            session.Action = PendingAction.ConfirmChangeTime;

            await UpdateSessionMessageAsync(
                chatId,
                session,
                "точно изменить время " + GetReminderTitle(session.Reminder) + " навсегда на " + FormatTime(time) + "?",
                ConfirmKeyboard(),
                ct);
        }

        private async Task HandlePostponeInputAsync(long chatId, AdminSession session, string text, CancellationToken ct)
        {
            DateTime dateTime;
            if (!TryParseMoscowDateTime(text, out dateTime))
            {
                await UpdateSessionMessageAsync(chatId, session, "не понял дату, введи например 2026-05-10 9:45 или 10.05.2026 9:45", CancelKeyboard(), ct);
                return;
            }

            if (dateTime <= NowMoscow())
            {
                await UpdateSessionMessageAsync(chatId, session, "это время уже в прошлом, введи заново", CancelKeyboard(), ct);
                return;
            }

            session.NewOneTimeAt = dateTime;
            session.Action = PendingAction.ConfirmPostpone;

            await UpdateSessionMessageAsync(
                chatId,
                session,
                "перенести ближайшее " + GetReminderTitle(session.Reminder) + " на " + FormatDateTime(dateTime) + "?",
                ConfirmKeyboard(),
                ct);
        }

        private async Task HandleConfirmationAsync(long chatId, int messageId, bool accepted, CancellationToken ct)
        {
            var session = GetActiveAdminSession();
            if (session == null || !IsConfirmationAction(session.Action))
            {
                await ShowAdminMenuAsync(chatId, ct, messageId, "действие уже неактуально");
                return;
            }

            if (!accepted)
            {
                ClearAdminSession();
                await ShowAdminMenuAsync(chatId, ct, messageId);
                return;
            }

            switch (session.Action)
            {
                case PendingAction.ConfirmDeleteMessage:
                    DeleteMessage(session.Bank, session.MessageIndex.GetValueOrDefault());
                    await SaveMessagesAsync();
                    await SaveStateAsync();
                    Console.WriteLine("Удалено сообщение из банка: " + GetBankTitle(session.Bank));
                    ClearAdminSession();
                    await ShowBankMenuAsync(chatId, ct, messageId, "удалил");
                    break;

                case PendingAction.ConfirmEditMessage:
                    GetBank(session.Bank)[session.MessageIndex.GetValueOrDefault()] = session.NewText ?? string.Empty;
                    await SaveMessagesAsync();
                    Console.WriteLine("Отредактировано сообщение в банке: " + GetBankTitle(session.Bank));
                    ClearAdminSession();
                    await ShowBankMenuAsync(chatId, ct, messageId, "сохранил");
                    break;

                case PendingAction.ConfirmSendNow:
                case PendingAction.ConfirmReplyToDarling:
                    var manualSendResult = await SendManualMessageToDarlingAsync(session.NewText ?? string.Empty, ct);
                    ClearAdminSession();
                    await ShowAdminMenuAsync(chatId, ct, messageId, manualSendResult);
                    break;

                case PendingAction.ConfirmSendFile:
                    var fileSendResult = await SendManualFileToDarlingAsync(session.SourceChatId.GetValueOrDefault(), session.SourceMessageId.GetValueOrDefault(), ct);
                    ClearAdminSession();
                    await ShowAdminMenuAsync(chatId, ct, messageId, fileSendResult);
                    break;

                case PendingAction.ConfirmChangeTime:
                    ApplyDailyTimeChange(session.Reminder, session.NewDailyTime.GetValueOrDefault());
                    await SaveStateAsync();
                    Console.WriteLine("Изменено время " + GetReminderTitle(session.Reminder) + ": " + GetReminder(session.Reminder).DailyTime);
                    ClearAdminSession();
                    await ShowReminderMenuAsync(chatId, session.Reminder, ct, messageId, "время изменено");
                    break;

                case PendingAction.ConfirmPostpone:
                    ApplyPostpone(session.Reminder, session.NewOneTimeAt.GetValueOrDefault());
                    await SaveStateAsync();
                    Console.WriteLine("Разовый перенос " + GetReminderTitle(session.Reminder) + ": " + FormatDateTime(session.NewOneTimeAt.GetValueOrDefault()));
                    ClearAdminSession();
                    await ShowReminderMenuAsync(chatId, session.Reminder, ct, messageId, "перенес");
                    break;

                case PendingAction.ConfirmSkipNext:
                    ApplySkipNext(session.Reminder);
                    await SaveStateAsync();
                    Console.WriteLine("Отменено ближайшее " + GetReminderTitle(session.Reminder));
                    ClearAdminSession();
                    await ShowReminderMenuAsync(chatId, session.Reminder, ct, messageId, "ближайшее сообщение отменено");
                    break;

                case PendingAction.ConfirmDisableReminder:
                    DisableReminder(session.Reminder);
                    await SaveStateAsync();
                    Console.WriteLine("Выключено: " + GetReminderTitle(session.Reminder));
                    ClearAdminSession();
                    await ShowReminderMenuAsync(chatId, session.Reminder, ct, messageId, "выключил");
                    break;

                case PendingAction.ConfirmRefreshAllBank:
                    session.Action = PendingAction.CollectRefreshMorning;
                    session.DraftMorning.Clear();
                    session.DraftNight.Clear();
                    await UpdateSessionMessageAsync(chatId, session, "присылай утренние сообщения", RefreshCollectKeyboard("refresh:morning_done"), ct);
                    break;
            }
        }

        private async Task HandleBankActionCallbackAsync(long chatId, int messageId, string data, CancellationToken ct)
        {
            var parts = data.Split(':');
            if (parts.Length < 3)
            {
                return;
            }

            var action = parts[1];
            var bank = ParseBank(parts[2]);

            if (action == "add")
            {
                await StartInputSessionAsync(chatId, messageId, new AdminSession(PendingAction.AddMessage) { Bank = bank }, "отправь новое сообщение для банка: " + GetBankTitle(bank), ct);
                return;
            }

            if (action == "delete")
            {
                await StartInputSessionAsync(chatId, messageId, new AdminSession(PendingAction.DeleteMessageNumber) { Bank = bank }, "введи номер сообщения, которое удалить", ct);
                return;
            }

            if (action == "edit")
            {
                await StartInputSessionAsync(chatId, messageId, new AdminSession(PendingAction.EditMessageNumber) { Bank = bank }, "введи номер сообщения, которое редактировать", ct);
            }
        }

        private async Task HandleViewBankCallbackAsync(long chatId, int messageId, string data, CancellationToken ct)
        {
            var parts = data.Split(':');
            if (parts.Length < 3)
            {
                return;
            }

            var bank = ParseBank(parts[1]);
            int page;
            if (!int.TryParse(parts[2], out page))
            {
                page = 0;
            }

            await ShowBankPageAsync(chatId, bank, page, ct, messageId);
        }

        private async Task HandleRefreshBankCallbackAsync(long chatId, int messageId, string data, CancellationToken ct)
        {
            var session = GetActiveAdminSession();
            if (session == null)
            {
                await ShowAdminMenuAsync(chatId, ct, messageId, "действие уже неактуально");
                return;
            }

            var parts = data.Split(':');
            var action = parts.Length > 1 ? parts[1] : string.Empty;

            if (action == "morning_done")
            {
                session.Action = PendingAction.CollectRefreshNight;
                await UpdateSessionMessageAsync(chatId, session, "присылай ночные сообщения", RefreshCollectKeyboard("refresh:night_done"), ct);
                return;
            }

            if (action == "night_done")
            {
                await ShowRefreshBankReviewAsync(chatId, ct, messageId, null, session);
                return;
            }

            if (action == "confirm")
            {
                _messages.Morning = new List<string>(session.DraftMorning);
                _messages.Night = new List<string>(session.DraftNight);
                _state.MorningQueue.Clear();
                _state.NightQueue.Clear();
                await SaveMessagesAsync();
                await SaveStateAsync();
                ClearAdminSession();
                await ShowBankMenuAsync(chatId, ct, messageId, "банк обновлен");
                return;
            }

            if (parts.Length < 3)
            {
                return;
            }

            var bank = ParseBank(parts[2]);
            session.Bank = bank;

            if (action == "add")
            {
                session.Action = PendingAction.RefreshAddText;
                await UpdateSessionMessageAsync(chatId, session, "отправь новое сообщение для черновика: " + GetBankTitle(bank), CancelKeyboard(), ct);
                return;
            }

            if (action == "delete")
            {
                session.Action = PendingAction.RefreshDeleteNumber;
                await UpdateSessionMessageAsync(chatId, session, "введи номер сообщения, которое удалить из черновика", CancelKeyboard(), ct);
                return;
            }

            if (action == "edit")
            {
                session.Action = PendingAction.RefreshEditNumber;
                await UpdateSessionMessageAsync(chatId, session, "введи номер сообщения, которое редактировать в черновике", CancelKeyboard(), ct);
            }
        }

        private async Task HandleRefreshDeleteNumberAsync(long chatId, AdminSession session, string text, CancellationToken ct)
        {
            var draft = GetDraftBank(session, session.Bank);
            int number;
            if (!int.TryParse(text.Trim(), out number) || number < 1 || number > draft.Count)
            {
                await UpdateSessionMessageAsync(chatId, session, "нет такого номера, введи заново", CancelKeyboard(), ct);
                return;
            }

            draft.RemoveAt(number - 1);
            await ShowRefreshBankReviewAsync(chatId, ct, session.PromptMessageId, "удалил", session);
        }

        private async Task HandleRefreshEditNumberAsync(long chatId, AdminSession session, string text, CancellationToken ct)
        {
            var draft = GetDraftBank(session, session.Bank);
            int number;
            if (!int.TryParse(text.Trim(), out number) || number < 1 || number > draft.Count)
            {
                await UpdateSessionMessageAsync(chatId, session, "нет такого номера, введи заново", CancelKeyboard(), ct);
                return;
            }

            session.MessageIndex = number - 1;
            session.Action = PendingAction.RefreshEditText;
            await UpdateSessionMessageAsync(chatId, session, "старый текст:\n\n" + draft[number - 1] + "\n\nотправь новый текст", CancelKeyboard(), ct);
        }

        private async Task HandleReminderCallbackAsync(long chatId, int messageId, string data, CancellationToken ct)
        {
            var parts = data.Split(':');
            if (parts.Length < 3)
            {
                return;
            }

            var reminder = ParseReminder(parts[1]);
            var action = parts[2];
            var reminderState = GetReminder(reminder);

            if (action == "postpone")
            {
                if (!reminderState.Enabled)
                {
                    await ShowReminderMenuAsync(chatId, reminder, ct, messageId, GetReminderTitle(reminder) + " выключено");
                    return;
                }

                await StartInputSessionAsync(chatId, messageId, new AdminSession(PendingAction.PostponeInput) { Reminder = reminder }, "введи дату и время переноса, например 2026-05-10 9:45 или 10.05.2026 9:45", ct);
                return;
            }

            if (action == "change")
            {
                await StartInputSessionAsync(chatId, messageId, new AdminSession(PendingAction.ChangeTimeInput) { Reminder = reminder }, "введи новое постоянное время, например 9:45 или 09:45", ct);
                return;
            }

            if (action == "skip")
            {
                if (!reminderState.Enabled)
                {
                    await ShowReminderMenuAsync(chatId, reminder, ct, messageId, GetReminderTitle(reminder) + " выключено");
                    return;
                }

                SetAdminSession(new AdminSession(PendingAction.ConfirmSkipNext) { Reminder = reminder });
                await EditOrSendTextAsync(chatId, messageId, "точно отменить ближайшее " + GetReminderTitle(reminder) + "?", ConfirmKeyboard(), ct);
                return;
            }

            if (action == "toggle")
            {
                if (reminderState.Enabled)
                {
                    SetAdminSession(new AdminSession(PendingAction.ConfirmDisableReminder) { Reminder = reminder });
                    await EditOrSendTextAsync(chatId, messageId, "точно выключить " + GetReminderTitle(reminder) + "?", ConfirmKeyboard(), ct);
                }
                else
                {
                    reminderState.Enabled = true;
                    reminderState.ClearOneTimeAndSkip();
                    await SaveStateAsync();
                    Console.WriteLine("Включено: " + GetReminderTitle(reminder));
                    await ShowReminderMenuAsync(chatId, reminder, ct, messageId, "включил");
                }
                return;
            }

            if (action == "test")
            {
                await SendTestMessageAsync(chatId, reminder, ct);
            }
        }

        private async Task RunSchedulerAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await CheckReminderAsync(ReminderKind.Morning, ct);
                    await CheckReminderAsync(ReminderKind.Night, ct);
                    await CheckDaily143Async(ct);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Ошибка расписания: " + ex);
                    if (_state.AdminUserId.HasValue)
                    {
                        await SafeSendTextAsync(_state.AdminUserId.Value, "ошибка расписания: " + ex.Message, ct);
                    }
                }

                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(30), ct);
                }
                catch (TaskCanceledException)
                {
                }
            }
        }

        private async Task CheckReminderAsync(ReminderKind kind, CancellationToken ct)
        {
            var reminder = GetReminder(kind);
            if (!reminder.Enabled)
            {
                return;
            }

            var now = NowMoscow();

            if (reminder.OneTimeAt.HasValue && reminder.OneTimeAt.Value < _startedAtMoscow)
            {
                Console.WriteLine("Устаревший разовый перенос пропущен: " + GetReminderTitle(kind));
                reminder.OneTimeAt = null;
                reminder.OneTimeReplacesDate = null;
                await SaveStateAsync();
            }

            if (reminder.OneTimeAt.HasValue && now >= reminder.OneTimeAt.Value)
            {
                await SendScheduledMessageAsync(kind, ct);
                reminder.OneTimeAt = null;
                reminder.OneTimeReplacesDate = null;
                await SaveStateAsync();
                return;
            }

            var dailyTime = ParseSavedTime(reminder.DailyTime);
            var todayDue = now.Date.Add(dailyTime);
            var todayKey = DateKey(now.Date);

            if (now >= todayDue &&
                todayDue >= _startedAtMoscow &&
                reminder.LastStandardSentDate != todayKey)
            {
                if (reminder.SkippedStandardDate == todayKey ||
                    reminder.OneTimeReplacesDate == todayKey ||
                    (reminder.OneTimeAt.HasValue && reminder.OneTimeAt.Value.Date == now.Date))
                {
                    reminder.LastStandardSentDate = todayKey;
                    if (reminder.SkippedStandardDate == todayKey)
                    {
                        reminder.SkippedStandardDate = null;
                    }
                    await SaveStateAsync();
                    return;
                }

                await SendScheduledMessageAsync(kind, ct);
                reminder.LastStandardSentDate = todayKey;
                await SaveStateAsync();
            }
        }

        private async Task CheckDaily143Async(CancellationToken ct)
        {
            if (!_state.Daily143Enabled)
            {
                return;
            }

            var now = NowMoscow();
            var due = now.Date.Add(Daily143Time);
            var todayKey = DateKey(now.Date);

            if (now < due ||
                due < _startedAtMoscow ||
                _state.Daily143LastHandledDate == todayKey)
            {
                return;
            }

            _state.Daily143LastHandledDate = todayKey;
            await SaveStateAsync();

            if (!_state.DarlingOutgoingEnabled)
            {
                Console.WriteLine(DarlingOutgoingDisabledText + ": 1:43");
                if (_state.AdminNotificationsEnabled && _state.AdminUserId.HasValue)
                {
                    await SafeSendTextAsync(_state.AdminUserId.Value, DarlingOutgoingDisabledText, ct);
                }
                return;
            }

            if (!_state.DarlingUserId.HasValue)
            {
                Console.WriteLine("Получатель не найден, отправка пропущена: 1:43");
                return;
            }

            try
            {
                await _bot.SendTextMessageAsync(_state.DarlingUserId.Value, "!", cancellationToken: ct);
                Console.WriteLine("Отправлено 1:43: !");

                if (_state.AdminNotificationsEnabled && _state.AdminUserId.HasValue)
                {
                    await SafeSendTextAsync(_state.AdminUserId.Value, "отправил 1:43:\n!", ct);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Ошибка отправки 1:43: " + ex.Message);
                if (_state.AdminUserId.HasValue)
                {
                    await SafeSendTextAsync(_state.AdminUserId.Value, "ошибка отправки зайке: " + ex.Message, ct);
                }
            }
        }

        private async Task SendScheduledMessageAsync(ReminderKind kind, CancellationToken ct)
        {
            if (!_state.DarlingOutgoingEnabled)
            {
                Console.WriteLine(DarlingOutgoingDisabledText + ": " + GetReminderTitle(kind));
                if (_state.AdminNotificationsEnabled && _state.AdminUserId.HasValue)
                {
                    await SafeSendTextAsync(_state.AdminUserId.Value, DarlingOutgoingDisabledText, ct);
                }
                return;
            }

            if (!_state.DarlingUserId.HasValue)
            {
                Console.WriteLine("Получатель не найден, отправка пропущена: " + GetReminderTitle(kind));
                return;
            }

            string text;
            try
            {
                text = GetNextMessage(kind);
            }
            catch (InvalidOperationException ex)
            {
                Console.WriteLine(ex.Message);
                if (_state.AdminUserId.HasValue)
                {
                    await SafeSendTextAsync(_state.AdminUserId.Value, ex.Message, ct);
                }
                return;
            }

            text = AddReminderHeart(text, kind);

            try
            {
                await _bot.SendTextMessageAsync(_state.DarlingUserId.Value, text, cancellationToken: ct);
                await SaveStateAsync();
                Console.WriteLine("Отправлено " + GetReminderTitle(kind) + ": " + text);

                if (_state.AdminNotificationsEnabled && _state.AdminUserId.HasValue)
                {
                    await SafeSendTextAsync(_state.AdminUserId.Value, "отправил " + GetReminderTitle(kind) + ":\n" + text, ct);
                    await SendBankRemainderWarningAsync(kind, ct);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Ошибка отправки " + GetReminderTitle(kind) + ": " + ex.Message);
                if (_state.AdminUserId.HasValue)
                {
                    await SafeSendTextAsync(_state.AdminUserId.Value, "ошибка отправки зайке: " + ex.Message, ct);
                }
            }
        }

        private async Task SendTestMessageAsync(long chatId, ReminderKind kind, CancellationToken ct)
        {
            var bank = GetBank(kind);
            if (bank.Count == 0)
            {
                await SafeSendTextAsync(chatId, "ошибка: " + GetReminderTitle(kind) + " банк сообщений пустой", ct);
                return;
            }

            var text = AddReminderHeart(bank[_random.Next(bank.Count)], kind);
            await SafeSendTextAsync(chatId, text, ct);
        }

        private async Task<string> SendManualMessageToDarlingAsync(string text, CancellationToken ct)
        {
            if (!_state.DarlingOutgoingEnabled)
            {
                return DarlingOutgoingDisabledText;
            }

            if (!_state.DarlingUserId.HasValue)
            {
                return "зайка еще не найдена";
            }

            try
            {
                await _bot.SendTextMessageAsync(_state.DarlingUserId.Value, text, cancellationToken: ct);
                _manualReplySinceLastDarlingMessage = true;
                return "отправил";
            }
            catch (Exception ex)
            {
                return "ошибка отправки зайке: " + ex.Message;
            }
        }

        private async Task<string> SendManualFileToDarlingAsync(long sourceChatId, int sourceMessageId, CancellationToken ct)
        {
            if (!_state.DarlingOutgoingEnabled)
            {
                return DarlingOutgoingDisabledText;
            }

            if (!_state.DarlingUserId.HasValue)
            {
                return "зайка еще не найдена";
            }

            try
            {
                await _bot.CopyMessageAsync(
                    chatId: _state.DarlingUserId.Value,
                    fromChatId: sourceChatId,
                    messageId: sourceMessageId,
                    cancellationToken: ct);
                _manualReplySinceLastDarlingMessage = true;
                return "отправил файл";
            }
            catch (Exception ex)
            {
                return "ошибка отправки файла зайке: " + ex.Message;
            }
        }

        private async Task ForwardDarlingMessageToAdminAsync(Message message, CancellationToken ct)
        {
            _manualReplySinceLastDarlingMessage = false;

            if (_state.AdminUserId.HasValue)
            {
                try
                {
                    await _bot.ForwardMessageAsync(_state.AdminUserId.Value, message.Chat.Id, message.MessageId, cancellationToken: ct);
                    await SafeSendTextAsync(_state.AdminUserId.Value, "ответить зайке?", ReplyKeyboard(), ct);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Не удалось переслать сообщение админу: " + ex.Message);
                }
            }
            else
            {
                Console.WriteLine("Админ не найден, пересылать сообщение некуда.");
            }

            ScheduleDarlingAck(message.Chat.Id);
        }

        private void ScheduleDarlingAck(long darlingChatId)
        {
            var oldCts = _darlingAckCts;
            if (oldCts != null)
            {
                oldCts.Cancel();
                oldCts.Dispose();
            }

            var cts = new CancellationTokenSource();
            _darlingAckCts = cts;

            Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);
                    if (!_manualReplySinceLastDarlingMessage)
                    {
                        if (!_state.DarlingOutgoingEnabled)
                        {
                            return;
                        }

                        await SafeSendTextAsync(darlingChatId, DarlingForwardAckText, cts.Token);
                    }
                }
                catch (TaskCanceledException)
                {
                }
            });
        }

        private async Task<UserRole> ResolveRoleAndRememberAsync(User user, CancellationToken ct)
        {
            if (_state.AdminUserId.HasValue && user.Id == _state.AdminUserId.Value)
            {
                return UserRole.Admin;
            }

            if (_state.DarlingUserId.HasValue && user.Id == _state.DarlingUserId.Value)
            {
                return UserRole.Darling;
            }

            if (user.Username == AdminUsername)
            {
                _state.AdminUserId = user.Id;
                await SaveStateAsync();
                return UserRole.Admin;
            }

            if (user.Username == DarlingUsername)
            {
                _state.DarlingUserId = user.Id;
                await SaveStateAsync();
                return UserRole.Darling;
            }

            return UserRole.Stranger;
        }

        private async Task ShowAdminMenuAsync(long chatId, CancellationToken ct, int? editMessageId = null, string? notice = null)
        {
            var keyboard = new InlineKeyboardMarkup(new[]
            {
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Статус", "menu:status"),
                    InlineKeyboardButton.WithCallbackData("Утро", "menu:morning")
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Ночь", "menu:night"),
                    InlineKeyboardButton.WithCallbackData("Банк сообщений", "menu:bank")
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Отправить зайке сейчас", "menu:send_now")
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Отправить файл зайке", "menu:send_file")
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Уведомления", "menu:notifications")
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("1:43 вкл/выкл", "menu:daily143")
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("отправлять сообщения вкл/выкл", "menu:darling_outgoing")
                }
            });

            var text = string.IsNullOrEmpty(notice) ? "админка" : notice + "\n\nадминка";
            await EditOrSendTextAsync(chatId, editMessageId, text, keyboard, ct);
        }

        private async Task ShowStatusAsync(long chatId, CancellationToken ct, int? editMessageId = null)
        {
            var lines = new List<string>
            {
                "Статус бота",
                "",
                FormatReminderStatus(ReminderKind.Morning),
                "",
                FormatReminderStatus(ReminderKind.Night),
                "",
                "1:43: " + (_state.Daily143Enabled ? "включено" : "выключено"),
                "отправка сообщений зайке: " + (_state.DarlingOutgoingEnabled ? "включена" : "выключена"),
                "Уведомления админу: " + (_state.AdminNotificationsEnabled ? "включены" : "выключены"),
                "админ найден: " + (_state.AdminUserId.HasValue ? "да" : "нет"),
                "зайка найдена: " + (_state.DarlingUserId.HasValue ? "да" : "нет")
            };

            await EditOrSendTextAsync(chatId, editMessageId, string.Join("\n", lines), BackToMainKeyboard(), ct);
        }

        private string FormatReminderStatus(ReminderKind kind)
        {
            var reminder = GetReminder(kind);
            var next = reminder.Enabled ? FormatDateTime(GetNextStandardDue(kind)) : "нет";
            return GetReminderTitle(kind) + ": " + (reminder.Enabled ? "включено" : "выключено") + "\n" +
                   "Время: " + reminder.DailyTime + "\n" +
                   "Ближайшая стандартная отправка: " + next + "\n" +
                   "Ближайшая отменена: " + (string.IsNullOrEmpty(reminder.SkippedStandardDate) ? "нет" : "да") + "\n" +
                   "Разовый перенос: " + (reminder.OneTimeAt.HasValue ? FormatDateTime(reminder.OneTimeAt.Value) : "нет");
        }

        private async Task ShowReminderMenuAsync(long chatId, ReminderKind kind, CancellationToken ct, int? editMessageId = null, string? notice = null)
        {
            var reminder = GetReminder(kind);
            var toggleText = reminder.Enabled ? "Выключить" : "Включить";
            var testText = kind == ReminderKind.Morning ? "Тест утра" : "Тест ночи";

            var keyboard = new InlineKeyboardMarkup(new[]
            {
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Перенести ближайшее", "rem:" + ReminderKey(kind) + ":postpone")
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Изменить время", "rem:" + ReminderKey(kind) + ":change"),
                    InlineKeyboardButton.WithCallbackData("Отменить ближайшее", "rem:" + ReminderKey(kind) + ":skip")
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData(toggleText, "rem:" + ReminderKey(kind) + ":toggle"),
                    InlineKeyboardButton.WithCallbackData(testText, "rem:" + ReminderKey(kind) + ":test")
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Назад", "menu:main")
                }
            });

            var text = string.IsNullOrEmpty(notice) ? GetReminderTitle(kind) : notice + "\n\n" + GetReminderTitle(kind);
            await EditOrSendTextAsync(chatId, editMessageId, text, keyboard, ct);
        }

        private async Task ShowBankMenuAsync(long chatId, CancellationToken ct, int? editMessageId = null, string? notice = null)
        {
            var keyboard = new InlineKeyboardMarkup(new[]
            {
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Посмотреть утренний банк", "view:morning:0")
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Посмотреть ночной банк", "view:night:0")
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Добавить утреннее", "bank:add:morning"),
                    InlineKeyboardButton.WithCallbackData("Добавить ночное", "bank:add:night")
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Редактировать утреннее", "bank:edit:morning"),
                    InlineKeyboardButton.WithCallbackData("Редактировать ночное", "bank:edit:night")
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Удалить утреннее", "bank:delete:morning"),
                    InlineKeyboardButton.WithCallbackData("Удалить ночное", "bank:delete:night")
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Обновить весь банк", "menu:refresh_bank")
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Назад", "menu:main")
                }
            });

            var text = string.IsNullOrEmpty(notice) ? "банк сообщений" : notice + "\n\nбанк сообщений";
            await EditOrSendTextAsync(chatId, editMessageId, text, keyboard, ct);
        }

        private async Task ShowBankPageAsync(long chatId, MessageBankKind bankKind, int page, CancellationToken ct, int? editMessageId = null)
        {
            var bank = GetBank(bankKind);
            const int pageSize = 10;
            var pageCount = Math.Max(1, (int)Math.Ceiling(bank.Count / (double)pageSize));
            page = Math.Max(0, Math.Min(page, pageCount - 1));

            var start = page * pageSize;
            var lines = new List<string>
            {
                GetBankTitle(bankKind) + " (" + bank.Count + ")",
                "страница " + (page + 1) + " из " + pageCount,
                ""
            };

            if (bank.Count == 0)
            {
                lines.Add("пусто");
            }
            else
            {
                for (var i = start; i < Math.Min(start + pageSize, bank.Count); i++)
                {
                    lines.Add((i + 1) + ". " + bank[i]);
                }
            }

            var buttons = new List<InlineKeyboardButton[]>();
            var nav = new List<InlineKeyboardButton>();
            if (page > 0)
            {
                nav.Add(InlineKeyboardButton.WithCallbackData("Назад", "view:" + BankKey(bankKind) + ":" + (page - 1)));
            }
            if (page < pageCount - 1)
            {
                nav.Add(InlineKeyboardButton.WithCallbackData("Вперед", "view:" + BankKey(bankKind) + ":" + (page + 1)));
            }
            if (nav.Count > 0)
            {
                buttons.Add(nav.ToArray());
            }
            buttons.Add(new[] { InlineKeyboardButton.WithCallbackData("В меню", "menu:bank") });

            await EditOrSendTextAsync(chatId, editMessageId, string.Join("\n", lines), new InlineKeyboardMarkup(buttons), ct);
        }

        private async Task ShowRefreshBankReviewAsync(long chatId, CancellationToken ct, int? editMessageId, string? notice, AdminSession session)
        {
            session.Action = PendingAction.RefreshReview;
            var lines = new List<string>();
            if (!string.IsNullOrEmpty(notice))
            {
                lines.Add(notice);
                lines.Add("");
            }

            lines.Add("черновик нового банка");
            lines.Add("");
            AppendDraftBankLines(lines, "утренний банк", session.DraftMorning);
            lines.Add("");
            AppendDraftBankLines(lines, "ночной банк", session.DraftNight);

            session.PromptMessageId = await EditOrSendTextAsync(chatId, editMessageId, string.Join("\n", lines), RefreshReviewKeyboard(), ct);
            SetAdminSession(session);
        }

        private static void AppendDraftBankLines(List<string> lines, string title, List<string> bank)
        {
            lines.Add(title + " (" + bank.Count + ")");
            if (bank.Count == 0)
            {
                lines.Add("пусто");
                return;
            }

            for (var i = 0; i < bank.Count; i++)
            {
                lines.Add((i + 1) + ". " + bank[i]);
            }
        }

        private InlineKeyboardMarkup ConfirmKeyboard()
        {
            return new InlineKeyboardMarkup(new[]
            {
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Да", "confirm:yes"),
                    InlineKeyboardButton.WithCallbackData("Нет", "confirm:no")
                }
            });
        }

        private InlineKeyboardMarkup RefreshCollectKeyboard(string doneCallback)
        {
            return new InlineKeyboardMarkup(new[]
            {
                new[]
                {
                    InlineKeyboardButton.WithCallbackData(doneCallback == "refresh:morning_done" ? "готово с утром" : "готово с ночью", doneCallback)
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Отмена", "flow:cancel")
                }
            });
        }

        private InlineKeyboardMarkup RefreshReviewKeyboard()
        {
            return new InlineKeyboardMarkup(new[]
            {
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("подтвердить банк", "refresh:confirm")
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("добавить утро", "refresh:add:morning"),
                    InlineKeyboardButton.WithCallbackData("добавить ночь", "refresh:add:night")
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("редактировать утро", "refresh:edit:morning"),
                    InlineKeyboardButton.WithCallbackData("редактировать ночь", "refresh:edit:night")
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("удалить утро", "refresh:delete:morning"),
                    InlineKeyboardButton.WithCallbackData("удалить ночь", "refresh:delete:night")
                },
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("отмена", "flow:cancel")
                }
            });
        }

        private InlineKeyboardMarkup ReplyKeyboard()
        {
            return new InlineKeyboardMarkup(new[]
            {
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Ответить зайке", "reply:darling")
                }
            });
        }

        private InlineKeyboardMarkup BackToMainKeyboard()
        {
            return new InlineKeyboardMarkup(new[]
            {
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Назад", "menu:main")
                }
            });
        }

        private InlineKeyboardMarkup CancelKeyboard()
        {
            return new InlineKeyboardMarkup(new[]
            {
                new[]
                {
                    InlineKeyboardButton.WithCallbackData("Отмена", "flow:cancel")
                }
            });
        }

        private ReplyKeyboardMarkup AdminReplyKeyboard()
        {
            return new ReplyKeyboardMarkup(new[]
            {
                new[]
                {
                    new KeyboardButton("Админка")
                }
            })
            {
                ResizeKeyboard = true
            };
        }

        private async Task StartInputSessionAsync(long chatId, int messageId, AdminSession session, string text, CancellationToken ct)
        {
            session.PromptMessageId = await EditOrSendTextAsync(chatId, messageId, text, CancelKeyboard(), ct);
            SetAdminSession(session);
        }

        private async Task UpdateSessionMessageAsync(long chatId, AdminSession session, string text, InlineKeyboardMarkup? replyMarkup, CancellationToken ct)
        {
            session.PromptMessageId = await EditOrSendTextAsync(chatId, session.PromptMessageId, text, replyMarkup, ct);
            SetAdminSession(session);
        }

        private async Task<int?> EditOrSendTextAsync(long chatId, int? messageId, string text, InlineKeyboardMarkup? replyMarkup, CancellationToken ct)
        {
            if (messageId.HasValue)
            {
                try
                {
                    await _bot.EditMessageTextAsync(
                        chatId: chatId,
                        messageId: messageId.Value,
                        text: text,
                        replyMarkup: replyMarkup,
                        cancellationToken: ct);
                    return messageId.Value;
                }
                catch (ApiRequestException ex)
                {
                    if (ex.Message.IndexOf("message is not modified", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        return messageId.Value;
                    }

                    Console.WriteLine("Не удалось отредактировать сообщение меню: " + ex.Message);
                }
            }

            var sent = await _bot.SendTextMessageAsync(
                chatId: chatId,
                text: text,
                replyMarkup: replyMarkup,
                cancellationToken: ct);
            return sent.MessageId;
        }

        private async Task SafeSendTextAsync(long chatId, string text, CancellationToken ct)
        {
            await SafeSendTextAsync(chatId, text, null, ct);
        }

        private async Task SafeSendTextAsync(long chatId, string text, IReplyMarkup? replyMarkup, CancellationToken ct)
        {
            await _bot.SendTextMessageAsync(
                chatId: chatId,
                text: text,
                replyMarkup: replyMarkup,
                cancellationToken: ct);
        }

        private string GetNextMessage(ReminderKind kind)
        {
            var bank = GetBank(kind);
            var queue = GetQueue(kind);

            if (bank.Count == 0)
            {
                throw new InvalidOperationException("ошибка: " + GetReminderTitle(kind) + " банк сообщений пустой");
            }

            if (bank.Count == 1)
            {
                return bank[0];
            }

            SanitizeQueue(queue, bank.Count);
            if (queue.Count == 0)
            {
                queue.AddRange(Enumerable.Range(0, bank.Count).OrderBy(_ => _random.Next()));
            }

            var index = queue[0];
            queue.RemoveAt(0);
            return bank[index];
        }

        private async Task SendBankRemainderWarningAsync(ReminderKind kind, CancellationToken ct)
        {
            if (!_state.AdminNotificationsEnabled || !_state.AdminUserId.HasValue)
            {
                return;
            }

            var bank = GetBank(kind);
            var remaining = bank.Count == 1 ? 0 : GetQueue(kind).Count;
            if (remaining < 0 || remaining > 3)
            {
                return;
            }

            var text = remaining == 0
                ? GetBankNominativeTitle(kind) + " банк закончился, следующий круг начнется с новым перемешиванием"
                : "в " + GetBankPrepositionalTitle(kind) + " банке осталось " + remaining + " " + FormatMessageWord(remaining);

            await SafeSendTextAsync(_state.AdminUserId.Value, text, ct);
        }

        private static string AddReminderHeart(string text, ReminderKind kind)
        {
            return text + " " + (kind == ReminderKind.Morning ? "🤍" : "🖤");
        }

        private void AddMessage(MessageBankKind bankKind, string text)
        {
            var bank = GetBank(bankKind);
            var queue = GetQueue(bankKind);
            bank.Add(text);
            var newIndex = bank.Count - 1;
            var insertAt = queue.Count == 0 ? 0 : _random.Next(queue.Count + 1);
            queue.Insert(insertAt, newIndex);
        }

        private void DeleteMessage(MessageBankKind bankKind, int index)
        {
            var bank = GetBank(bankKind);
            var queue = GetQueue(bankKind);
            if (index < 0 || index >= bank.Count)
            {
                return;
            }

            bank.RemoveAt(index);
            for (var i = queue.Count - 1; i >= 0; i--)
            {
                if (queue[i] == index)
                {
                    queue.RemoveAt(i);
                }
                else if (queue[i] > index)
                {
                    queue[i]--;
                }
            }
        }

        private void SanitizeQueue(List<int> queue, int bankCount)
        {
            var seen = new HashSet<int>();
            for (var i = queue.Count - 1; i >= 0; i--)
            {
                var index = queue[i];
                if (index < 0 || index >= bankCount || !seen.Add(index))
                {
                    queue.RemoveAt(i);
                }
            }
        }

        private void ApplyDailyTimeChange(ReminderKind kind, TimeSpan time)
        {
            var reminder = GetReminder(kind);
            reminder.DailyTime = FormatTime(time);
            reminder.ClearOneTimeAndSkip();
        }

        private void ApplyPostpone(ReminderKind kind, DateTime oneTimeAt)
        {
            var reminder = GetReminder(kind);
            reminder.OneTimeAt = oneTimeAt;
            reminder.OneTimeReplacesDate = DateKey(GetNextStandardDue(kind).Date);
            reminder.SkippedStandardDate = null;
        }

        private void ApplySkipNext(ReminderKind kind)
        {
            var reminder = GetReminder(kind);
            if (reminder.OneTimeAt.HasValue)
            {
                reminder.SkippedStandardDate = reminder.OneTimeReplacesDate;
                reminder.OneTimeAt = null;
                reminder.OneTimeReplacesDate = null;
                return;
            }

            reminder.SkippedStandardDate = DateKey(GetNextStandardDue(kind).Date);
        }

        private void DisableReminder(ReminderKind kind)
        {
            var reminder = GetReminder(kind);
            reminder.Enabled = false;
            reminder.ClearOneTimeAndSkip();
        }

        private DateTime GetNextStandardDue(ReminderKind kind)
        {
            var reminder = GetReminder(kind);
            var now = NowMoscow();
            var time = ParseSavedTime(reminder.DailyTime);
            var due = now.Date.Add(time);
            if (due <= now)
            {
                due = due.AddDays(1);
            }

            if (reminder.SkippedStandardDate == DateKey(due.Date))
            {
                due = due.AddDays(1);
            }

            return due;
        }

        private void CleanupExpiredOneTimeOverrides()
        {
            var now = NowMoscow();
            CleanupExpiredOneTimeOverride(_state.Morning, now, "утро");
            CleanupExpiredOneTimeOverride(_state.Night, now, "ночь");
        }

        private static void CleanupExpiredOneTimeOverride(ReminderState reminder, DateTime now, string title)
        {
            if (reminder.OneTimeAt.HasValue && reminder.OneTimeAt.Value < now)
            {
                Console.WriteLine("Устаревший разовый перенос очищен при запуске: " + title);
                reminder.OneTimeAt = null;
                reminder.OneTimeReplacesDate = null;
            }
        }

        private AdminSession? GetActiveAdminSession()
        {
            lock (_sessionLock)
            {
                if (_adminSession == null)
                {
                    return null;
                }

                if (DateTime.UtcNow - _adminSession.UpdatedAtUtc > TimeSpan.FromMinutes(10))
                {
                    _adminSession = null;
                    return null;
                }

                return _adminSession;
            }
        }

        private void SetAdminSession(AdminSession session)
        {
            lock (_sessionLock)
            {
                session.Touch();
                _adminSession = session;
            }
        }

        private void ClearAdminSession()
        {
            lock (_sessionLock)
            {
                _adminSession = null;
            }
        }

        private static bool IsConfirmationAction(PendingAction action)
        {
            return action == PendingAction.ConfirmDeleteMessage ||
                   action == PendingAction.ConfirmEditMessage ||
                   action == PendingAction.ConfirmSendNow ||
                   action == PendingAction.ConfirmReplyToDarling ||
                   action == PendingAction.ConfirmSendFile ||
                   action == PendingAction.ConfirmChangeTime ||
                   action == PendingAction.ConfirmPostpone ||
                   action == PendingAction.ConfirmSkipNext ||
                   action == PendingAction.ConfirmDisableReminder ||
                   action == PendingAction.ConfirmRefreshAllBank;
        }

        private static bool HasSendableFileContent(Message message)
        {
            return message.Audio != null ||
                   message.Document != null ||
                   message.Photo != null ||
                   message.Video != null ||
                   message.Voice != null ||
                   message.VideoNote != null ||
                   message.Animation != null ||
                   message.Sticker != null;
        }

        private ReminderState GetReminder(ReminderKind kind)
        {
            return kind == ReminderKind.Morning ? _state.Morning : _state.Night;
        }

        private List<string> GetBank(ReminderKind kind)
        {
            return kind == ReminderKind.Morning ? _messages.Morning : _messages.Night;
        }

        private List<string> GetBank(MessageBankKind kind)
        {
            return kind == MessageBankKind.Morning ? _messages.Morning : _messages.Night;
        }

        private static List<string> GetDraftBank(AdminSession session, MessageBankKind kind)
        {
            return kind == MessageBankKind.Morning ? session.DraftMorning : session.DraftNight;
        }

        private List<int> GetQueue(ReminderKind kind)
        {
            return kind == ReminderKind.Morning ? _state.MorningQueue : _state.NightQueue;
        }

        private List<int> GetQueue(MessageBankKind kind)
        {
            return kind == MessageBankKind.Morning ? _state.MorningQueue : _state.NightQueue;
        }

        private DateTime NowMoscow()
        {
            return TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, _moscowTimeZone);
        }

        private async Task SaveStateAsync()
        {
            await _storage.SaveStateAsync(_state);
        }

        private async Task SaveMessagesAsync()
        {
            await _storage.SaveMessagesAsync(_messages);
        }

        private static string GetReminderTitle(ReminderKind kind)
        {
            return kind == ReminderKind.Morning ? "утро" : "ночь";
        }

        private static string GetBankNominativeTitle(ReminderKind kind)
        {
            return kind == ReminderKind.Morning ? "утренний" : "ночной";
        }

        private static string GetBankPrepositionalTitle(ReminderKind kind)
        {
            return kind == ReminderKind.Morning ? "утреннем" : "ночном";
        }

        private static string FormatMessageWord(int count)
        {
            return count == 1 ? "сообщение" : "сообщения";
        }

        private static string GetBankTitle(MessageBankKind kind)
        {
            return kind == MessageBankKind.Morning ? "утренний банк" : "ночной банк";
        }

        private static string ReminderKey(ReminderKind kind)
        {
            return kind == ReminderKind.Morning ? "morning" : "night";
        }

        private static string BankKey(MessageBankKind kind)
        {
            return kind == MessageBankKind.Morning ? "morning" : "night";
        }

        private static ReminderKind ParseReminder(string value)
        {
            return value == "morning" ? ReminderKind.Morning : ReminderKind.Night;
        }

        private static MessageBankKind ParseBank(string value)
        {
            return value == "morning" ? MessageBankKind.Morning : MessageBankKind.Night;
        }

        private static bool TryParseTime(string text, out TimeSpan time)
        {
            return TimeSpan.TryParseExact(text.Trim(), new[] { "h\\:mm", "hh\\:mm" }, CultureInfo.InvariantCulture, out time);
        }

        private static bool TryParseMoscowDateTime(string text, out DateTime dateTime)
        {
            var formats = new[]
            {
                "yyyy-MM-dd H:mm",
                "yyyy-MM-dd HH:mm",
                "dd.MM.yyyy H:mm",
                "dd.MM.yyyy HH:mm"
            };

            return DateTime.TryParseExact(
                text.Trim(),
                formats,
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out dateTime);
        }

        private static TimeSpan ParseSavedTime(string text)
        {
            TimeSpan time;
            return TryParseTime(text, out time) ? time : TimeSpan.Zero;
        }

        private static string FormatTime(TimeSpan time)
        {
            return time.ToString(@"hh\:mm", CultureInfo.InvariantCulture);
        }

        private static string FormatDateTime(DateTime dateTime)
        {
            return dateTime.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture);
        }

        private static string DateKey(DateTime date)
        {
            return date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }
    }

    internal sealed class BotConfig
    {
        public string TelegramBotToken { get; set; } = string.Empty;
        public string BotDataDir { get; set; } = string.Empty;

        public static BotConfig Load()
        {
            var config = new BotConfig();
            var localConfigPath = Path.Combine(Directory.GetCurrentDirectory(), "appsettings.local.json");

            if (IOFile.Exists(localConfigPath))
            {
                var fileConfig = JsonSerializer.Deserialize<BotConfig>(IOFile.ReadAllText(localConfigPath));
                if (fileConfig != null)
                {
                    config = fileConfig;
                }
            }

            var tokenFromEnv = Environment.GetEnvironmentVariable("TELEGRAM_BOT_TOKEN");
            if (!string.IsNullOrWhiteSpace(tokenFromEnv))
            {
                config.TelegramBotToken = tokenFromEnv;
            }

            var dataDirFromEnv = Environment.GetEnvironmentVariable("BOT_DATA_DIR");
            if (!string.IsNullOrWhiteSpace(dataDirFromEnv))
            {
                config.BotDataDir = dataDirFromEnv;
            }

            if (string.IsNullOrWhiteSpace(config.BotDataDir))
            {
                config.BotDataDir = Directory.GetCurrentDirectory();
            }

            return config;
        }
    }

    internal sealed class JsonStorage
    {
        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            WriteIndented = true,
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };

        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        public JsonStorage(string dataDir)
        {
            DataDir = Path.GetFullPath(dataDir);
        }

        public string DataDir { get; }

        private string StatePath { get { return Path.Combine(DataDir, "state.json"); } }

        private string MessagesPath { get { return Path.Combine(DataDir, "messages.json"); } }

        public async Task<BotState> LoadStateAsync()
        {
            Directory.CreateDirectory(DataDir);
            if (!IOFile.Exists(StatePath))
            {
                var state = BotState.CreateDefault();
                await SaveStateAsync(state);
                Console.WriteLine("Создан state.json");
                return state;
            }

            var json = await IOFile.ReadAllTextAsync(StatePath);
            return JsonSerializer.Deserialize<BotState>(json, JsonOptions) ?? BotState.CreateDefault();
        }

        public async Task<MessageBank> LoadMessagesAsync()
        {
            Directory.CreateDirectory(DataDir);
            if (!IOFile.Exists(MessagesPath))
            {
                var templatePath = Path.Combine(Directory.GetCurrentDirectory(), "messages.json");
                MessageBank messages;
                if (IOFile.Exists(templatePath) && !PathsEqual(templatePath, MessagesPath))
                {
                    var json = await IOFile.ReadAllTextAsync(templatePath);
                    messages = JsonSerializer.Deserialize<MessageBank>(json, JsonOptions) ?? MessageBank.CreateDefault();
                }
                else
                {
                    messages = MessageBank.CreateDefault();
                }

                await SaveMessagesAsync(messages);
                Console.WriteLine("Создан messages.json");
                return messages;
            }

            var existingJson = await IOFile.ReadAllTextAsync(MessagesPath);
            return JsonSerializer.Deserialize<MessageBank>(existingJson, JsonOptions) ?? MessageBank.CreateDefault();
        }

        public async Task SaveStateAsync(BotState state)
        {
            await SaveJsonAsync(StatePath, state);
        }

        public async Task SaveMessagesAsync(MessageBank messages)
        {
            await SaveJsonAsync(MessagesPath, messages);
        }

        private async Task SaveJsonAsync<T>(string path, T value)
        {
            await _lock.WaitAsync();
            try
            {
                Directory.CreateDirectory(DataDir);
                var json = JsonSerializer.Serialize(value, JsonOptions);
                await IOFile.WriteAllTextAsync(path, json);
            }
            finally
            {
                _lock.Release();
            }
        }

        private static bool PathsEqual(string first, string second)
        {
            return string.Equals(Path.GetFullPath(first).TrimEnd(Path.DirectorySeparatorChar), Path.GetFullPath(second).TrimEnd(Path.DirectorySeparatorChar), StringComparison.OrdinalIgnoreCase);
        }
    }

    internal sealed class BotState
    {
        public long? AdminUserId { get; set; }
        public long? DarlingUserId { get; set; }
        public bool AdminNotificationsEnabled { get; set; } = true;
        public bool Daily143Enabled { get; set; } = true;
        public string? Daily143LastHandledDate { get; set; }
        public bool DarlingOutgoingEnabled { get; set; } = true;
        public ReminderState Morning { get; set; } = new ReminderState { DailyTime = "10:00" };
        public ReminderState Night { get; set; } = new ReminderState { DailyTime = "03:00" };
        public List<int> MorningQueue { get; set; } = new List<int>();
        public List<int> NightQueue { get; set; } = new List<int>();

        public static BotState CreateDefault()
        {
            return new BotState();
        }
    }

    internal sealed class ReminderState
    {
        public bool Enabled { get; set; } = true;
        public string DailyTime { get; set; } = "10:00";
        public string? LastStandardSentDate { get; set; }
        public DateTime? OneTimeAt { get; set; }
        public string? OneTimeReplacesDate { get; set; }
        public string? SkippedStandardDate { get; set; }

        public void ClearOneTimeAndSkip()
        {
            OneTimeAt = null;
            OneTimeReplacesDate = null;
            SkippedStandardDate = null;
        }
    }

    internal sealed class MessageBank
    {
        public List<string> Morning { get; set; } = new List<string>();
        public List<string> Night { get; set; } = new List<string>();

        public static MessageBank CreateDefault()
        {
            return new MessageBank
            {
                Morning = new List<string>
                {
                    "привет зайка! проснулись улыбнулись, и с желанием убивать на работку",
                    "утречка <3, к кротам как относишься?",
                    "доброе утро, за. как настроение у тя, владику расскажи",
                    "❝ Я ненавидел утра. Они напоминали мне, что у ночи бывает конец и что нужно вновь как-то справляться со своими мыслями. ❞",
                    "„Уинстон, да вы пьяны! — Всё верно. А вы уродина. Завтра утром я протрезвею. А вы так и останетесь уродиной.“",
                    "наступило утро, и мы опять живем",
                    "просыпаемся!!!",
                    "это не первое, и не последнее утро 143",
                    "мы слишком молоды чтобы вести себя мудро, я знаю что говорю!!!",
                    "опять утро, ну((("
                },
                Night = new List<string>
                {
                    "засыпай, за. владику потом расскажешь как спалось",
                    "доброй ночи зайка. пусть сны будут мягкие, а будильник утром пойдет нахуй",
                    "вот и день опять прошёл, ну и нахуй он пошёл! завтра будет день опять, ну и в рот его ебать. дембель стал на день короче, пацанам спокойной ночи!",
                    "спаьсплю",
                    "владос попросил передать: целую!",
                    "кружок владику запиши и спаьсплю!",
                    "ночь и тишина данная на век",
                    "аххахахпзапахпазпхахпа",
                    "быстренько спать оп, спокойной ночи!",
                    "сладких снов, зайка",
                    "але",
                    "спишь?)"
                }
            };
        }
    }

    internal sealed class AdminSession
    {
        public AdminSession(PendingAction action)
        {
            Action = action;
            UpdatedAtUtc = DateTime.UtcNow;
        }

        public PendingAction Action { get; set; }
        public MessageBankKind Bank { get; set; }
        public ReminderKind Reminder { get; set; }
        public int? MessageIndex { get; set; }
        public int? PromptMessageId { get; set; }
        public long? SourceChatId { get; set; }
        public int? SourceMessageId { get; set; }
        public string? NewText { get; set; }
        public TimeSpan? NewDailyTime { get; set; }
        public DateTime? NewOneTimeAt { get; set; }
        public List<string> DraftMorning { get; } = new List<string>();
        public List<string> DraftNight { get; } = new List<string>();
        public DateTime UpdatedAtUtc { get; private set; }

        public void Touch()
        {
            UpdatedAtUtc = DateTime.UtcNow;
        }
    }

    internal enum UserRole
    {
        Stranger,
        Admin,
        Darling
    }

    internal enum ReminderKind
    {
        Morning,
        Night
    }

    internal enum MessageBankKind
    {
        Morning,
        Night
    }

    internal enum PendingAction
    {
        AddMessage,
        DeleteMessageNumber,
        ConfirmDeleteMessage,
        EditMessageNumber,
        EditMessageText,
        ConfirmEditMessage,
        SendNow,
        ConfirmSendNow,
        SendFile,
        ConfirmSendFile,
        ReplyToDarling,
        ConfirmReplyToDarling,
        ChangeTimeInput,
        ConfirmChangeTime,
        PostponeInput,
        ConfirmPostpone,
        ConfirmSkipNext,
        ConfirmDisableReminder,
        ConfirmRefreshAllBank,
        CollectRefreshMorning,
        CollectRefreshNight,
        RefreshReview,
        RefreshAddText,
        RefreshDeleteNumber,
        RefreshEditNumber,
        RefreshEditText
    }

    internal static class TimeZoneHelper
    {
        public static TimeZoneInfo GetMoscowTimeZone()
        {
            try
            {
                return TimeZoneInfo.FindSystemTimeZoneById("Russian Standard Time");
            }
            catch (TimeZoneNotFoundException)
            {
                return TimeZoneInfo.FindSystemTimeZoneById("Europe/Moscow");
            }
        }
    }
}
