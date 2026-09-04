import asyncio
import os
import urllib.request
import yaml

# 上游源地址
SOURCE_URL = (
    "https://raw.githubusercontent.com/sinspired/VpngateAPI/main/vpngate.yaml"
)

# 中转友好度最高的 TCP 常用端口
PREFERRED_TCP_PORTS = {443, 80, 8443, 995, 1194, 1195}


def get_node_priority(node):
    """计算节点优先级 (数字越小优先级越高，用于排序)

    1: TCP + 常用端口 (443/80/8443 等)
    2: TCP + 其他端口
    3: UDP 协议
    """
    proto = str(
        node.get("Proto") or node.get("proto") or "tcp"
    ).lower()
    try:
        port = int(
            node.get("Port")
            or node.get("port")
            or node.get("openvpn_port", 0)
        )
    except (ValueError, TypeError):
        port = 0

    if proto == "tcp":
        if port in PREFERRED_TCP_PORTS:
            return 1  # 最高优先级
        return 2  # 普通 TCP
    return 3  # UDP 垫底


# 1. 下载并解析远程 YAML 文件
def fetch_source_nodes(url):
    print(f"正在从上游拉取节点列表: {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as response:
        content = response.read().decode("utf-8")
        data = yaml.safe_load(content)

    if isinstance(data, list):
        return data
    elif isinstance(data, dict):
        return data.get("proxies", data.get("nodes", []))
    return []


# 2. 异步 TCP/UDP 连通性测试
async def check_port(ip, port, proto="tcp", timeout=2.0):
    if not ip or not port:
        return False
    try:
        port = int(port)
        if str(proto).lower() == "udp":
            loop = asyncio.get_running_loop()
            transport, _ = await asyncio.wait_for(
                loop.create_datagram_endpoint(
                    lambda: asyncio.DatagramProtocol(), remote_addr=(ip, port)
                ),
                timeout=timeout,
            )
            transport.close()
            return True
        else:
            conn = asyncio.open_connection(ip, port)
            _, writer = await asyncio.wait_for(conn, timeout=timeout)
            writer.close()
            await writer.wait_closed()
            return True
    except Exception:
        return False


# 3. 排序、筛选并探测节点
async def get_live_nodes(candidates, count=10, country_tag=""):
    print(f"\n--- 开始测试 {country_tag} 节点 (目标: {count} 个活节点) ---")

    # 按优先级重新排序：优先测试 TCP + 443/80/1194 等高兼容端口
    sorted_candidates = sorted(candidates, key=get_node_priority)

    live_list = []

    for node in sorted_candidates:
        if len(live_list) >= count:
            break

        ip = node.get("IP") or node.get("ip") or node.get("server")
        port = (
            node.get("Port")
            or node.get("port")
            or node.get("openvpn_port", 1194)
        )
        proto = node.get("Proto") or node.get("proto", "tcp")

        p_level = get_node_priority(node)
        priority_tag = (
            "★ TCP常用" if p_level == 1 else ("TCP" if p_level == 2 else "UDP")
        )

        is_alive = await check_port(ip, port, proto, timeout=2.0)
        if is_alive:
            live_list.append(node)
            print(
                f"✓ [{country_tag} 存活 | {priority_tag}] {node.get('name') or node.get('HostName') or ip}:{port} ({proto})"
            )

    return live_list


async def main():
    try:
        nodes = fetch_source_nodes(SOURCE_URL)
        print(f"成功获取节点总数: {len(nodes)}")
    except Exception as e:
        print(f"拉取节点失败: {e}")
        return

    jp_candidates = []
    kr_candidates = []

    # 按国家过滤 JP / KR
    for node in nodes:
        country = str(
            node.get("Country")
            or node.get("country")
            or node.get("CountryShort")
            or node.get("CountryLong")
            or node.get("name")
            or ""
        ).upper()

        if "JP" in country or "JAPAN" in country:
            jp_candidates.append(node)
        elif "KR" in country or "KOREA" in country:
            kr_candidates.append(node)

    print(f"找到 JP 候选节点: {len(jp_candidates)} 个")
    print(f"找到 KR 候选节点: {len(kr_candidates)} 个")

    # 按优先级测试并筛选前 10 个活节点
    live_jp = await get_live_nodes(jp_candidates, 10, "JP")
    live_kr = await get_live_nodes(kr_candidates, 10, "KR")

    # 输出合并
    output_data = {"proxies": live_jp + live_kr}

    output_file = "live_vpngate.yaml"
    with open(output_file, "w", encoding="utf-8") as f:
        yaml.dump(output_data, f, allow_unicode=True, sort_keys=False)

    print(
        f"\n[完成] 已写入 {len(live_jp)} 个 JP 活节点和 {len(live_kr)} 个 KR 活节点至 {output_file}"
    )


if __name__ == "__main__":
    asyncio.run(main())
