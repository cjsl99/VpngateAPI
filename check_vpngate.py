import asyncio
import os
import yaml


def load_nodes(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# 异步测试 TCP / UDP 端口连通性
async def check_port(ip, port, proto="tcp", timeout=2.0):
    if not ip or not port:
        return False
    port = int(port)
    try:
        if str(proto).lower() == "udp":
            # UDP 简单探测（建立 socket 试探）
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
            # TCP 建立连接
            conn = asyncio.open_connection(ip, port)
            _, writer = await asyncio.wait_for(conn, timeout=timeout)
            writer.close()
            await writer.wait_closed()
            return True
    except Exception:
        return False


async def main():
    input_file = "vpngate.yaml"  # 你的 2000+ 节点源文件名
    if not os.path.exists(input_file):
        print(f"未找到输入文件: {input_file}")
        return

    nodes = load_nodes(input_file)
    if not isinstance(nodes, list):
        # 如果 yaml 嵌套在字典里，按实际结构调整，如 nodes = nodes.get('proxies', [])
        print("YAML 结构非列表，请根据实际格式调整解析逻辑")
        return

    jp_candidates = []
    kr_candidates = []

    # 1. 过滤 JP / KR
    for item in nodes:
        # 兼容不同 YAML 属性命名习惯 (Country / country / country_code)
        country = (
            item.get("Country")
            or item.get("country")
            or item.get("CountryLong", "")
        )
        country_code = item.get("CountryShort") or country

        if "JP" in str(country_code).upper() or "JAPAN" in str(country).upper():
            jp_candidates.append(item)
        elif (
            "KR" in str(country_code).upper() or "KOREA" in str(country).upper()
        ):
            kr_candidates.append(item)

    print(f"筛选到 JP 节点 {len(jp_candidates)} 个，KR 节点 {len(kr_candidates)} 个")

    # 2. 并发探测并各筛选 10 个活节点
    async def filter_live_nodes(candidates, count=10):
        live_list = []
        for node in candidates:
            if len(live_list) >= count:
                break
            ip = node.get("IP") or node.get("ip") or node.get("server")
            port = node.get("Port") or node.get("port", 1194)
            proto = node.get("Proto") or node.get("proto", "tcp")

            is_alive = await check_port(ip, port, proto, timeout=2.0)
            if is_alive:
                live_list.append(node)
                print(
                    f"✓ [存活] {node.get('CountryShort', 'NODE')} - {ip}:{port} ({proto})"
                )
        return live_list

    print("\n--- 正在测试 JP 节点 ---")
    live_jp = await filter_live_nodes(jp_candidates, 10)

    print("\n--- 正在测试 KR 节点 ---")
    live_kr = await filter_live_nodes(kr_candidates, 10)

    # 3. 输出为精简版的 live_vpngate.yaml
    output_data = {"jp_nodes": live_jp, "kr_nodes": live_kr}

    with open("live_vpngate.yaml", "w", encoding="utf-8") as f:
        yaml.dump(output_data, f, allow_unicode=True, sort_keys=False)

    print(
        f"\n完成！已挑选并保存 {len(live_jp)} 个 JP 节点和 {len(live_kr)} 个 KR 节点至 live_vpngate.yaml"
    )


if __name__ == "__main__":
    asyncio.run(main())
