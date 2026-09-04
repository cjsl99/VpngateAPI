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
    """端口优先级排序：1=TCP常用端口, 2=TCP普通端口, 3=UDP"""
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
    """调用 ip-api.com 判断是否为住宅 IP (hosting==False 且 proxy==False)"""
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
            is_hosting = data.get("hosting", True)  # True 为机房 IP
            is_proxy = data.get("proxy", False)  # True 为已被标记代理
            is_residential = (not is_hosting) and (not is_proxy)
            isp_info = data.get("isp", "") or data.get("org", "")
            return is_residential, f"ISP: {isp_info} | 机房IP: {is_hosting}"
    except Exception:
        pass
    return False, "纯净度检测超时"


async def get_live_nodes(candidates, count=10, country_tag=""):
    print(f"\n--- 开始测试 {country_tag} 节点 (优先搜寻 {count} 个住宅 IP) ---")

    sorted_candidates = sorted(candidates, key=get_node_priority)

    pure_residential_nodes = []  # 住宅 IP 队列
    datacenter_nodes = []  # 机房 IP 备用队列

    max_api_calls = 35  # 设置查询上限，避免过度占用接口
    api_calls = 0

    for node in sorted_candidates:
        # 如果已经收集齐了 10 个住宅 IP，直接提前结束测试
        if len(pure_residential_nodes) >= count:
            break

        # 如果达到了查询上限，且总活节点数已够 10 个，也结束测试
        if (
            api_calls >= max_api_calls
            and (len(pure_residential_nodes) + len(datacenter_nodes)) >= count
        ):
            break

        server = node.get("server")
        port = node.get("port")
        proto = node.get("proto", "tcp")

        if not server or not port:
            continue

        # 1. 端口连通性检查
        is_alive = await check_port(server, port, proto, timeout=2.0)
        if is_alive:
            # 2. IP 住宅纯净度检查
            is_residential, purity_msg = await check_ip_purity(server)
            api_calls += 1
            await asyncio.sleep(1.3)  # 避开 45次/分钟 的 API 限速

            node_name = node.get("name", server)

            if is_residential:
                pure_residential_nodes.append(node)
                print(
                    f"✓ [{country_tag} 存活 | ★ 住宅IP] {node_name} -> {purity_msg}"
                )
            else:
                datacenter_nodes.append(node)
                print(
                    f"✓ [{country_tag} 存活 | 机房IP] {node_name} -> {purity_msg}"
                )

    # 3. 结果合并：住宅 IP 绝对排在最前面，不足时用机房 IP 补齐至 count 个
    final_nodes = (pure_residential_nodes + datacenter_nodes)[:count]

    print(
        f"[{country_tag} 筛选结果] 共选中 {len(final_nodes)} 个节点 (包含住宅 IP: {len(pure_residential_nodes)} 个, 机房 IP: {len(final_nodes) - len(pure_residential_nodes)} 个)"
    )

    return final_nodes


async def main():
    try:
        nodes = fetch_source_nodes(SOURCE_URL)
        print(f"成功获取上游节点总数: {len(nodes)}")
    except Exception as e:
        print(f"拉取节点失败: {e}")
        return

    jp_candidates = []
    kr_candidates = []

    # 按名称前缀精确匹配国家 (如 VPNGate-JP-...)
    for node in nodes:
        name = str(node.get("name", ""))
        parts = name.split("-")

        country_code = parts[1].upper() if len(parts) >= 2 else ""

        if country_code == "JP":
            jp_candidates.append(node)
        elif country_code == "KR":
            kr_candidates.append(node)

    print(f"找到 JP 候选节点: {len(jp_candidates)} 个")
    print(f"找到 KR 候选节点: {len(kr_candidates)} 个")

    live_jp = await get_live_nodes(jp_candidates, 10, "JP")
    live_kr = await get_live_nodes(kr_candidates, 10, "KR")

    # 保持原格式输出
    output_data = {"proxies": live_jp + live_kr}

    output_file = "live_vpngate.yaml"
    with open(output_file, "w", encoding="utf-8") as f:
        yaml.dump(output_data, f, allow_unicode=True, sort_keys=False)

    print(f"\n[完成] 已将测试结果写入 {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
