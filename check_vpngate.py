import asyncio
import json
import urllib.request
import yaml

# 上游源地址
SOURCE_URL = (
    "https://raw.githubusercontent.com/sinspired/VpngateAPI/main/vpngate.yaml"
)

# 常用高兼容 TCP 端口
PREFERRED_TCP_PORTS = {443, 80, 8443, 995, 1194, 1195}


def get_node_priority(node):
    """优先级计算：1=TCP常用端口, 2=TCP普通端口, 3=UDP"""
    proto = str(node.get("proto", "tcp")).lower()
    try:
        port = int(node.get("port", 0))
    except (ValueError, TypeError):
        port = 0

    if proto == "tcp":
        if port in PREFERRED_TCP_PORTS:
            return 1
        return 2
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


async def check_ip_purity(ip):
    """调用 ip-api.com 查询 IP 纯净度"""
    url = f"http://ip-api.com/json/{ip}?fields=status,message,countryCode,isp,org,as,proxy,hosting"
    try:
        loop = asyncio.get_running_loop()
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0"}
        )

        def do_request():
            with urllib.request.urlopen(req, timeout=3) as resp:
                return json.loads(resp.read().decode("utf-8"))

        data = await loop.run_in_executor(None, do_request)

        if data.get("status") == "success":
            is_hosting = data.get("hosting", True)
            is_proxy = data.get("proxy", False)
            is_pure = (not is_hosting) and (not is_proxy)
            isp_info = data.get("isp", "") or data.get("org", "")
            return is_pure, f"ISP: {isp_info} | 机房IP: {is_hosting}"
    except Exception:
        pass
    return False, "纯净度检测超时"


async def get_live_nodes(candidates, count=10, country_tag=""):
    print(f"\n--- 开始测试 {country_tag} 节点 (目标: {count} 个活节点) ---")

    sorted_candidates = sorted(candidates, key=get_node_priority)
    live_list = []

    for node in sorted_candidates:
        if len(live_list) >= count:
            break

        server = node.get("server")
        port = node.get("port")
        proto = node.get("proto", "tcp")

        if not server or not port:
            continue

        # 1. 端口连通性测试
        is_alive = await check_port(server, port, proto, timeout=2.0)
        if is_alive:
            # 2. IP 纯净度检测
            is_pure, purity_msg = await check_ip_purity(server)
            await asyncio.sleep(1.3)  # 避开 ip-api 的 45次/分 限速

            purity_tag = "住宅" if is_pure else "机房"

            # 3. 核心修复：同步重写 node['name']，保证客户端显示名称与实际连接 IP/端口 100% 对齐
            node["name"] = (
                f"VPNGate-{country_tag}-{purity_tag}-{server}-{port}-{proto}"
            )

            live_list.append(node)
            print(
                f"✓ [{country_tag} 存活 | {purity_tag}] {server}:{port} ({proto}) -> {purity_msg}"
            )

    return live_list


async def main():
    try:
        nodes = fetch_source_nodes(SOURCE_URL)
        print(f"成功获取上游节点总数: {len(nodes)}")
    except Exception as e:
        print(f"拉取节点失败: {e}")
        return

    jp_candidates = []
    kr_candidates = []

    # 精确匹配：从 VPNGate-JP-219.100.37.211-443-tcp 中精准切分出国家代码 JP
    for node in nodes:
        name = str(node.get("name", ""))
        parts = name.split("-")

        # 提取 name 里的第二个位置作为国家代码 (例如 parts[1] == "JP")
        country_code = parts[1].upper() if len(parts) >= 2 else ""

        if country_code == "JP":
            jp_candidates.append(node)
        elif country_code == "KR":
            kr_candidates.append(node)

    print(f"精准找到 JP 候选节点: {len(jp_candidates)} 个")
    print(f"精准找到 KR 候选节点: {len(kr_candidates)} 个")

    live_jp = await get_live_nodes(jp_candidates, 10, "JP")
    live_kr = await get_live_nodes(kr_candidates, 10, "KR")

    output_data = {"proxies": live_jp + live_kr}

    output_file = "live_vpngate.yaml"
    with open(output_file, "w", encoding="utf-8") as f:
        yaml.dump(output_data, f, allow_unicode=True, sort_keys=False)

    print(
        f"\n[完成] 已写入 {len(live_jp)} 个 JP 活节点和 {len(live_kr)} 个 KR 活节点至 {output_file}"
    )


if __name__ == "__main__":
    asyncio.run(main())
