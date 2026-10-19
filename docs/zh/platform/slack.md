# 接入 Slack

## 创建 AstrBot Slack 平台适配器

进入 `机器人` 页，点击 `+ 创建机器人`，找到 Slack 并点击进入 Slack 配置页。

![选择 Slack 消息平台](/slack/image-1.png?v=2)

## 在 Slack 创建 App

Slack 支持两种接入方式：`Webhook` 与 `Socket`。如果您没有公网服务器并且消息业务量的规模较小，我们建议您使用 `socket` 方式。如果您有公网服务器（或者有一定的技术背景，了解如何设置 Tunnel，如 Cloudflare Tunnel），可以选择 `webhook` 方式。`socket` 方式部署相对简单。

1. 创建 [Slack](https://slack.com/signin) 账号和一个工作区（Workspace）。
2. 前往 [应用后台](https://api.slack.com/apps)，点击「Create New App」->「Blank app」->「Continue」。输入 `应用名称` 和要添加到的工作区，然后点击「Create」。

3. （仅 Webhook 需要）获取 `Signing Secret`，在左边栏 Basic Information 页下，找到 App Credentials 的 `Signing Secret`，点击 Show 并且复制到平台适配器配置的 signing_secret 处。

![image](https://files.astrbot.app/docs/source/images/slack/image.png)

4. 在左边栏 Basic Information 页下，找到 App-Level Tokens，点击 「Generate Token and Scopes」。Token Name 任意输入，点击 Add Scope，选择 `connections:write`，然后点击 「Generate」，点击 Copy 将结果复制到 AstrBot 配置页的 app_token 处。

![image](https://files.astrbot.app/docs/source/images/slack/image-2.png)

5. 在左边栏 OAuth & Permissions 页下，在 Bot Token Scopes 下方添加如下权限：
   - channels:history
   - channels:read
   - channels:write.invites
   - chat:write
   - chat:write.customize
   - chat:write.public
   - files:read
   - files:write
   - groups:history
   - groups:read
   - groups:write
   - im:history
   - im:read
   - im:write
   - mpim:read
   - reactions:read
   - reactions:write
   - users:read

   订阅 `member_joined_channel` 事件时，Slack 会自动要求 `mpim:read`，用于读取机器人已加入的群组私聊的基本信息。

6. 在左边栏 OAuth & Permissions 页下，在 Oauth Token 处点击 `Install to xxx`（xxx 是您工作区的名字）。然后复制生成的 Bot User OAuth Token 到平台适配器配置的 bot_token 处。

7. （仅 Socket 需要）在左边栏 Socket Mode 页下，开启 Enable Socket Mode。

![image](https://files.astrbot.app/docs/source/images/slack/image-3.png)

## 启动平台适配器

现在，配置已经完成。如果您使用的是 Socket 模式，那么直接点击配置的右下角的保存按钮即可。

如果您使用的是 Webhook 模式，请保持 `统一 Webhook 模式 (unified_webhook_mode)` 为开启状态。

> [!TIP]
> v4.8.0 之前没有 `统一 Webhook 模式`，请填写以下配置项：
> Slack Webhook Host、Slack Webhook Port 和 Slack Webhook Path

## 开启事件接收

新建平台适配器成功后，返回到 Slack 设置，在左边栏 Event Subscriptions 页下，点击 Enable Events 启用事件接收。

如果您使用的是 Webhook 模式：

- 如果开启了 `统一 Webhook 模式`，点击保存之后，AstrBot 将会自动为你生成唯一的 Webhook 回调链接，你可以在 `数据与日志 → 日志` 中查看，或在 `机器人` 页选中该机器人，点击 `查看 Webhook 链接`，将该链接填入 `Request URL` 输入框中。

- 如果没有开启 `统一 Webhook 模式`，请在 `Request URL` 输入框中输入 `https://您的域名/astrbot-slack-webhook/callback`。

> [!TIP]
> Webhook 模式下，您需要先在 DNS 服务商处设置好域名，然后使用反向代理软件将请求转发到 AstrBot 所在服务器的 `6185` 端口（如果开启了统一 Webhook 模式）或配置指定的端口（如果没有开启统一 Webhook 模式）。或者您可以使用 Cloudflare Tunnel。具体教程请参考网络资源，本教程不赘述。

启用后，在下方的 Subscribe to bot events 处，点击 Add Bot User Event，添加如下事件：

1. channel_created
2. channel_deleted
3. channel_left
4. member_joined_channel
5. member_left_channel
6. message.channels
7. message.groups
8. message.im
9. reaction_added
10. reaction_removed
11. team_join

添加完成后，点击页面底部的 `Save Changes` 保存事件订阅，再进行下面的测试。

## 原生工具进度卡片（实验性）

在 Slack 适配器设置中开启 **Slack Native Task Cards**
（`slack_native_task_cards`，默认关闭），并在当前配置中开启流式回复和工具使用展示。
回复已有线程内的消息时，AstrBot 会用一张原生任务卡片展示工具开始和结果，正文独立发送。
卡片只显示工具名称和状态，不展示参数及返回内容。
原生卡片和文字进度中的工具名称统一压成单行，最多 120 个字符，超长时以省略号结尾。

频道顶层消息、非流式回复和实时 TTS 保持原有的工具进度展示方式。
关闭工具使用展示时也不显示卡片。工具异常、超时和显式错误会显示失败；
中断且未确认完成的任务不会标记为成功。后台工具的完成表示提交成功，不代表后台工作已经结束。

Slack 明确拒绝卡片能力或格式时降级为一条文字进度消息，后续任务状态持续更新这条消息。
文字消息发送或更新失败时停止进度更新，不另发消息，正文仍正常发送。其他发送故障会停止当前回复的卡片更新，
不重试发送结果不明的请求，也不阻止正文发送。Slack 明确告知卡片流已关闭时，
会在同一线程尝试新建卡片并携带当前任务状态，每次更新最多续卡一次，旧卡片保留。
续卡失败时遵循上述降级规则。

文字进度只展示最近开始的 8 项任务，较早任务按状态汇总，包含失败和未完成数量；
内部仍保留全部任务状态，较早任务的后续结果也会更新汇总。
收尾时，未完成任务显示为“状态未确认”。原生卡片保留任务历史，使用 Slack 自带的折叠展示。

进度接口限流不会暂停工具执行。AstrBot 会在 `Retry-After` 期间保留最新任务状态，
冷却结束后的下一次进度事件会同步这些状态。回复结束时，原生卡片尝试携带最终状态关闭；
文字进度若仍在冷却期则保留当前显示，不安排后台重试。正文发送的限流策略保持不变。

任务卡片和文字进度的每次请求（包括收尾）最多等待 3 秒，超时按发送结果不明处理，
不重发、不另建进度消息，工具执行和正文发送继续。正文请求的超时策略保持不变。

## 测试成功性

进入您刚刚添加的 Slack 工作区，进入需要用到 Bot 的频道，然后 @ 您刚刚创建的应用。然后点击 Slackbot 随后发送的消息中的 添加 按钮来添加到工作区中。然后，@ 应用，输入 `/help`，如果能够成功回复，说明测试成功。

如果有疑问，请[提交 Issue](https://github.com/AstrBotDevs/AstrBot/issues)。
