import asyncio
import urllib.request
import yaml

SOURCE_URL = (
    "https://raw.githubusercontent.com/sinspired/VpngateAPI/main/vpngate.yaml"
)
PREFERRED_TCP_PORTS = {443, 80, 8443, 995, 1194, 1195}

# 免测直过、全量保留地区 (香港, 台湾, 澳门)
DIRECT_ADD_REGIONS = {"HK", "TW", "MO"}

# 1. 热门亚太地区 (每国配额 5 个，去除已直过的 HK, TW)
POPULAR_APAC = {"JP", "KR", "SG", "TH", "VN", "ID", "PH", "MY"}

# 2. 亚太以外热门地区 (每国配额 3 个)
POPULAR_NON_APAC = {"US", "GB", "UK", "DE", "FR", "CA", "AU", "RU", "NL"}


def extract_country_code(node):
    """从节点名称中提取国家代码（例如 VPNGate-US-xxx -> US）"""
    name = str(node.get("name", ""))
    parts = name.split("-")
    if len(parts) >= 2:
        return parts[1].upper()
    return "UNKNOWN"


def get_country_limit(country_code):
    """根据地区分类返回对应的节点配额"""
    if country_code in POPULAR_APAC:
        return 5, "热门亚太"
    elif country_code in POPULAR_NON_APAC:
        return 3, "亚太外热门"
    else:
        return 2, "其它地区"


def get_node_priority(node):
    """端口优先级排序：1=TCP常用端口, 2=TCP普通端口, 3=UDP"""
    proto = str(node.get("proto", "tcp")).lower()
    try:
        port = int(node.get("port", 0))
    except (ValueError, TypeError):
        port = 0

    if proto == "tcp":
        return 1 if port in PREFERRED_TCP_PORTS else 2
    return 3


def fetch_source_nodes(url):
    print(f"正在拉取上游节点列表: {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as response:
        content = response.read().decode("utf-8")
        data = yaml.safe_load(content)

    if isinstance(data, dict):
        return data.get("proxies", [])
    elif isinstance(data, list):
        return data
    return []


async def check_port(node, timeout=2.0):
    """异步检查单个节点端口连通性"""
    ip = node.get("server")
    port = node.get("port")
    proto = str(node.get("proto", "tcp")).lower()

    if not ip or not port:
        return node, False

    try:
        port = int(port)
        if proto == "udp":
            loop = asyncio.get_running_loop()
            transport, _ = await asyncio.wait_for(
                loop.create_datagram_endpoint(
                    lambda: asyncio.DatagramProtocol(), remote_addr=(ip, port)
                ),
                timeout=timeout,
            )
            transport.close()
            return node, True
        else:
            conn = asyncio.open_connection(ip, port)
            _, writer = await asyncio.wait_for(conn, timeout=timeout)
            writer.close()
            await writer.wait_closed()
            return node, True
    except Exception:
        return node, False


async def main():
    try:
        nodes = fetch_source_nodes(SOURCE_URL)
        print(f"成功获取上游节点总数: {len(nodes)}")
    except Exception as e:
        print(f"拉取节点失败: {e}")
        return

    # 按国家/地区将所有候选节点归类分组
    country_groups = {}
    for node in nodes:
        cc = extract_country_code(node)
        if cc not in country_groups:
            country_groups[cc] = []
        country_groups[cc].append(node)

    print(
        f"解析到上游包含 {len(country_groups)} 个国家/地区的节点: {list(country_groups.keys())}"
    )

    selected_nodes = []

    # 1. 优先提取免测直过地区 (HK, TW, MO) 的全部节点
    print("\n--- 提取免测直过地区 (HK, TW, MO) 的全部节点 ---")
    for cc in sorted(DIRECT_ADD_REGIONS):
        if cc in country_groups:
            direct_nodes = country_groups[cc]
            selected_nodes.extend(direct_nodes)
            print(
                f"★ [{cc} | 免测直过] 不测活直接提取全部 {len(direct_nodes)} 个节点"
            )
            for p in direct_nodes:
                print(f"   └─ {p.get('name')}")

    # 2. 对其它国家的节点进行端口探针并发测试
    test_country_groups = {
        cc: candidates
        for cc, candidates in country_groups.items()
        if cc not in DIRECT_ADD_REGIONS
    }

    print("\n--- 开始其余国家/地区节点端口连通性并发测试 ---")
    all_tasks = []
    node_to_cc_map = []

    for cc, candidates in test_country_groups.items():
        sorted_candidates = sorted(candidates, key=get_node_priority)
        for node in sorted_candidates:
            all_tasks.append(check_port(node))
            node_to_cc_map.append((node, cc))

    results = await asyncio.gather(*all_tasks)

    # 收集测试成功的存活节点
    live_country_buckets = {}
    for (node, cc), (test_node, is_alive) in zip(node_to_cc_map, results):
        if is_alive:
            if cc not in live_country_buckets:
                live_country_buckets[cc] = []
            live_country_buckets[cc].append(node)

    # 按地区配额精细化选取节点
    sorted_country_codes = sorted(
        live_country_buckets.keys(),
        key=lambda code: (
            0
            if code in POPULAR_APAC
            else (1 if code in POPULAR_NON_APAC else 2),
            code,
        ),
    )

    print("\n--- 其它地区节点选取与配额执行结果 ---")
    for cc in sorted_country_codes:
        nodes_list = live_country_buckets[cc]
        limit, tier_name = get_country_limit(cc)
        picked = nodes_list[:limit]
        selected_nodes.extend(picked)

        print(
            f"✓ [{tier_name} | {cc}] 存活 {len(nodes_list)} 个 -> 提取前 {len(picked)} 个 (上限 {limit})"
        )
        for p in picked:
            print(f"   └─ {p.get('name')}")

    output_data = {"proxies": selected_nodes}

    output_file = "live_vpngate.yaml"
    with open(output_file, "w", encoding="utf-8") as f:
        yaml.dump(output_data, f, allow_unicode=True, sort_keys=False)

    print(
        f"\n[完成] 共成功提取 {len(selected_nodes)} 个节点，已保存至 {output_file}"
    )


if __name__ == "__main__":
    asyncio.run(main())
