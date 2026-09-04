import asyncio
import urllib.request
import yaml

SOURCE_URL = (
    "https://raw.githubusercontent.com/sinspired/VpngateAPI/main/vpngate.yaml"
)
PREFERRED_TCP_PORTS = {443, 80, 8443, 995, 1194, 1195}


def extract_country_code(node):
    """从节点名称中提取国家代码（例如 VPNGate-US-xxx -> US）"""
    name = str(node.get("name", ""))
    parts = name.split("-")
    if len(parts) >= 2:
        return parts[1].upper()
    return "UNKNOWN"


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


async def filter_live_nodes(
    candidates, count=10, country_tag="", enforce_diversity=False
):
    print(f"\n--- 开始筛选 {country_tag} 节点 (目标 {count} 个) ---")

    # 按端口优先级排序候选节点
    sorted_candidates = sorted(candidates, key=get_node_priority)

    print(f"[{country_tag}] 正在并发测试 {len(sorted_candidates)} 个节点端口...")
    tasks = [check_port(node) for node in sorted_candidates]
    port_results = await asyncio.gather(*tasks)

    # 提取所有端口连通的活节点
    live_candidates = [node for node, is_alive in port_results if is_alive]
    print(
        f"[{country_tag}] 端口测试完成，共探测到 {len(live_candidates)} 个存活节点"
    )

    # 如果不需要国家多样性打散（如 JP 和 KR），按顺序直接截取前 count 个
    if not enforce_diversity:
        selected = live_candidates[:count]
        for node in selected:
            print(f"✓ [{country_tag} 选中] {node.get('name')}")
        return selected

    # 开启【国家多样性轮询算法】
    country_buckets = {}
    for node in live_candidates:
        cc = extract_country_code(node)
        if cc not in country_buckets:
            country_buckets[cc] = []
        country_buckets[cc].append(node)

    print(
        f"[{country_tag}] 存活节点覆盖 {len(country_buckets)} 个国家/地区: {list(country_buckets.keys())}"
    )

    selected = []
    countries = list(country_buckets.keys())

    # 轮询抽取：第 1 轮每个国家拿 1 个，第 2 轮每个国家再拿 1 个... 保证分布均匀
    while len(selected) < count and any(country_buckets.values()):
        for cc in countries:
            if country_buckets[cc]:
                picked_node = country_buckets[cc].pop(0)
                selected.append(picked_node)
                print(
                    f"✓ [{country_tag} 选中 | {cc}] {picked_node.get('name')}"
                )
                if len(selected) >= count:
                    break

    print(f"[{country_tag} 完成] 成功多样化挑选出 {len(selected)} 个可用节点")
    return selected


async def main():
    try:
        nodes = fetch_source_nodes(SOURCE_URL)
        print(f"成功获取上游节点总数: {len(nodes)}")
    except Exception as e:
        print(f"拉取节点失败: {e}")
        return

    jp_candidates = []
    kr_candidates = []
    other_candidates = []

    # 按国家代码对节点分类
    for node in nodes:
        cc = extract_country_code(node)
        if cc == "JP":
            jp_candidates.append(node)
        elif cc == "KR":
            kr_candidates.append(node)
        else:
            other_candidates.append(node)

    print(f"找到 JP 候选节点: {len(jp_candidates)} 个")
    print(f"找到 KR 候选节点: {len(kr_candidates)} 个")
    print(f"找到 其他国家/地区候选节点: {len(other_candidates)} 个")

    # 异步并发同时测试 JP, KR, OTHERS 三组
    # 对 OTHERS 开启 enforce_diversity=True 强制按国家轮询平均抽取
    live_jp, live_kr, live_others = await asyncio.gather(
        filter_live_nodes(jp_candidates, 10, "JP", enforce_diversity=False),
        filter_live_nodes(kr_candidates, 10, "KR", enforce_diversity=False),
        filter_live_nodes(
            other_candidates, 50, "OTHERS", enforce_diversity=True
        ),
    )

    all_live_nodes = live_jp + live_kr + live_others
    output_data = {"proxies": all_live_nodes}

    output_file = "live_vpngate.yaml"
    with open(output_file, "w", encoding="utf-8") as f:
        yaml.dump(output_data, f, allow_unicode=True, sort_keys=False)

    print(
        f"\n[完成] 共生成 {len(all_live_nodes)} 个节点 (JP: {len(live_jp)}, KR: {len(live_kr)}, 其他多国均衡: {len(live_others)})，已写入 {output_file}"
    )


if __name__ == "__main__":
    asyncio.run(main())
