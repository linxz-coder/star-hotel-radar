# 星级酒店捡漏雷达

输入目的地、目标酒店和入住日期，系统筛选目标酒店 3 公里内的四星级以上酒店，并按同类型日期价格判断是否适合捡漏。

## 功能

- 工作日只对比工作日，周末只对比周末
- 自动生成对比日期；普通日期会自动避开公众假期，公众假期只在同一假期内对比
- 当前价比对比均价便宜 100 元以上时进入“适合捡漏的酒店”
- 默认先查 3 公里；如果没有捡漏酒店，会自动扩展到 5 公里，再没有则扩展到 10 公里
- 单独展示希尔顿、万豪、雅高、温德姆、洲际、凯悦、香格里拉、凯宾斯基、文华东方、岭南东方
- 支持最低价、最高价、星级、半径、排序筛选
- 酒店中文名优先使用 Trip.com/携程/国内地图等标准中文名；没有标准名时再用繁体转简体或英文规则兜底
- 相同搜索条件默认缓存 7 天；切换排序会复用缓存并重排，不重复抓取；超过 7 天会重新查询 Trip.com 实时价格
- Trip.com 搜索默认渐进式返回：先返回任务进度；有缓存先秒出缓存，有候选先展示候选，后台继续刷新实时价格和完整比价
- 支持对最近热搜城市/酒店做 MySQL 定时预热，热门搜索可直接命中缓存
- 数据层使用 Provider 接口：页面默认使用 `TripComProvider` 实时查询；本地样例/导入数据接口保留给后续人工导入

## 日期与假期规则

公众假期使用参考项目同源的 `www.iamwawa.cn/workingday/api`，并内置 2026 年官方假期作为兜底。比如用户选择 2026-05-01 劳动节，系统只会从 2026-05-01 至 2026-05-05 这段假期里选对比日期；用户选择普通周末时，会跳过 2026-06-19 至 2026-06-21 端午假期这类公众假期日期。

## 搜索缓存

搜索缓存会优先写入本地 MySQL 的 `hotel_search_cache` 表，同时保留 `.cache/search_cache` 文件缓存作为兜底。缓存 key 包含城市、目标酒店、入住日期、半径、星级、价格区间、数据源和规则版本，不包含排序字段，所以同一批搜索结果切换“优惠/价格/距离/星级”排序不会重新搜索。

酒店中文名另有独立缓存：已核验成功的中文名会按 `Trip.com hotelId` 优先写入 MySQL 的 `hotel_name_cache` 表，同时写入 `.cache/hotel_name_cache.json` 文件缓存。下次同一酒店出现在任何搜索结果里，会先直接套用已确认中文名，不再重复访问携程、艺龙、地图或搜索引擎做中文名核验。

Trip.com 实时搜索结果默认缓存 7 天。7 天是硬 TTL，超过后会自动丢弃旧缓存并重新抓取；另外有 12 小时软刷新窗口：命中缓存时会先把缓存结果秒返回给用户，如果缓存已经较旧或缓存本身只是 partial 结果，系统会在后台刷新 Trip.com 实时价格并覆盖 MySQL。

缓存是渐进式的：后台冷抓只要拿到至少 1 家目标日期候选酒店，就会先把 partial 结果写入 MySQL 和文件缓存。下一次同条件搜索会先展示这些已知候选，并沿着这个缓存继续后台补完整比价，避免用户每次都从空白等待开始。0 候选的结果不会入缓存，也不会作为命中结果返回，防止 Trip.com 波动时把空结果缓存住。

MySQL 默认配置：

```text
HOST=127.0.0.1
PORT=3306
USER=root
PASSWORD=
DATABASE=star_hotel_deal_app
TABLE=hotel_search_cache
NAME_TABLE=hotel_name_cache
```

可用环境变量覆盖：

```bash
export HOTEL_DEAL_MYSQL_HOST=127.0.0.1
export HOTEL_DEAL_MYSQL_PORT=3306
export HOTEL_DEAL_MYSQL_USER=root
export HOTEL_DEAL_MYSQL_PASSWORD=''
export HOTEL_DEAL_MYSQL_DATABASE=star_hotel_deal_app
export HOTEL_DEAL_MYSQL_ENABLED=1
export HOTEL_DEAL_MYSQL_NAME_TABLE=hotel_name_cache
```

如果 MySQL 不可用，应用会自动退回文件缓存和实时搜索，不会阻断用户搜索。导入酒店数据后，本地数据源缓存会自动清空。

## 热门搜索预热

预热脚本会读取“最近热搜 + 默认热门目标”，为热门城市/酒店提前查询 Trip.com 并写入 MySQL 和文件缓存。默认只预热应用默认入住日期，可用 `--dates` 指定多个日期。

手动轻量预热一个目标：

```bash
python3 scripts/prewarm_mysql_cache.py --limit 1 --mode quick --dates 2026-06-01 --sleep 0
```

完整预热前 6 个热门目标：

```bash
python3 scripts/prewarm_mysql_cache.py --limit 6 --mode full
```

launchd 定时任务配置在：

```text
deploy/launchd/com.linxz.star-hotel-deal.prewarm.plist
```

当前配置为每 6 小时运行一次，日志写入：

```text
.cache/prewarm.log
.cache/prewarm.err.log
```

可用环境变量调整：

```bash
export HOTEL_DEAL_PREWARM_LIMIT=6
export HOTEL_DEAL_PREWARM_DATES=2026-06-01,2026-06-05
export HOTEL_DEAL_PREWARM_MODE=full
export HOTEL_DEAL_TRIPCOM_REFRESH_AFTER_SECONDS=43200
```

## 启动

```bash
python3 app.py
```

本地地址：

```text
http://127.0.0.1:5013
```

后台监控页：

```text
http://127.0.0.1:5013/admin
```

## 测试

```bash
python3 -m pytest -q
```

## Trip.com 实时模式

页面默认使用 `TripComProvider`：

1. Trip.com 关键词搜索定位目标酒店
2. 通过酒店列表页响应抓取候选酒店和价格
3. 对每个对比日期重复抓取价格
4. 本地算法再过滤 3 公里、四星级以上和价格区间

为了让用户更早看到结果，前端默认使用渐进式异步搜索：`/api/search` 会立即返回 `summary.jobId` 和 `summary.progress`；前端随后轮询 `/api/search/status/<jobId>`。如果缓存命中，会先展示缓存并用 `summary.refreshJobId` 后台刷新；如果冷抓中间拿到候选酒店，会先展示目标日期候选和当前价；后台完整比价完成后自动替换为最终结果。需要同步调试时可传 `asyncMode=0`。

Trip.com 页面结构和风控可能变化。后续可以把 `providers.py` 内的 `TripComProvider` 替换为携程、Booking、Agoda 或自有数据库实现。

## 中文名核验来源

Trip.com 候选酒店会先展示，中文名在后台逐步核验。当前顺序：

```text
Trip.com 已有中文名 -> Trip.com 详情页 -> 携程中文页 -> 艺龙酒店列表 -> 高德/百度 POI -> 搜索引擎 -> 繁体/英文规则兜底
```

艺龙只作为中文名核验来源，不参与实时价格和候选酒店搜索。没有配置凭证时会自动跳过。可选环境变量：

```bash
export HOTEL_DEAL_ELONG_USER=your_user
export HOTEL_DEAL_ELONG_APP_KEY=your_app_key
export HOTEL_DEAL_ELONG_SECRET_KEY=your_secret_key
export HOTEL_DEAL_ELONG_REGION_ID_MAP='{"深圳":"1314","上海":"0201"}'
```

如艺龙账号使用不同网关或方法名，可覆盖：

```bash
export HOTEL_DEAL_ELONG_API_HOST=https://api.elong.com/rest
export HOTEL_DEAL_ELONG_METHOD=ihotel.list
```

## 导入数据

POST `/api/import`：

```json
{
  "hotels": [
    {
      "hotelId": "custom-1",
      "hotelName": "广州天河希尔顿酒店",
      "city": "广州",
      "starRating": 5,
      "latitude": 23.1439,
      "longitude": 113.3272,
      "basePrice": 860,
      "brand": "Hilton",
      "group": "Hilton Worldwide",
      "imageUrl": "",
      "tripUrl": ""
    }
  ]
}
```

导入文件会保存到 `.cache/imported_hotels.json`，不会覆盖 `data/sample_hotels.json`。

## 本机公网 Tunnel

```bash
scripts/start_tunnel.sh
```

脚本会检查本地 Flask 服务，然后启动 Cloudflare Quick Tunnel 并输出公网地址。

手机访问后台时，使用脚本输出的“手机后台地址”，格式是：

```text
https://xxxx.trycloudflare.com/admin
```

如果以后设置了 `HOTEL_DEAL_ADMIN_TOKEN`，访问后台时需要在地址后追加 `?token=你的token`。
