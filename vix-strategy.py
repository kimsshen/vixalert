import yfinance as yf
import pandas as pd
from datetime import datetime
import requests

def safe_extract_close(df, ticker):
    """安全提取 Close 列，兼容 MultiIndex"""
    if df.empty:
        raise ValueError("DataFrame is empty")
    
    # 检查是否为 MultiIndex 列
    if isinstance(df.columns, pd.MultiIndex):
        return df[('Close', ticker)]
    else:
        return df['Close']

def send_wechat_notification(title: str, content: str, send_key: str):
    """
    使用 Server酱 发送微信通知
    :param title: 消息标题（必填）
    :param content: 消息内容（支持 Markdown）
    :param send_key: 你的 SendKey
    """
    url = f"https://sctapi.ftqq.com/{send_key}.send"
    data = {
        "title": title,
        "desp": content  # 支持 Markdown，如换行用 \n\n，加粗用 **text**
    }
    response = requests.post(url, data=data)
    result = response.json()
    
    if result.get("code") == 0:
        print("✅ 微信通知发送成功！")
    else:
        print(f"❌ 发送失败: {result.get('message')}")


def get_latest_sp500_pe():
    """从 multpl.com 获取最新的 S&P 500 PE Ratio"""
    url = "https://www.multpl.com/s-p-500-pe-ratio/table/by-month"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        # 使用 pandas 读取表格
        tables = pd.read_html(response.text)
        if not tables:
            return None
        
        df = tables[0]
        # 获取第一行第二列的值，处理可能存在的换行符和特殊符号 (†)
        raw_val = str(df.iloc[0, 1])
        pe_str = raw_val.replace('†', '').strip().split()[-1]
        return float(pe_str)
    except Exception as e:
        print(f"⚠️ 无法获取 S&P 500 PE Ratio: {e}")
        return None


def main():

    SEND_KEY = "SCT312240T75M1tG903ZKOzaKdA42lgr8n"
    
    try:
        # 1. 直接下载最近90个交易日数据（避免手动日期计算）
        spy_raw = yf.download("SPY", period="90d", progress=False)
        vix_raw = yf.download("^VIX", period="90d", progress=False)

        # 2. 安全提取 Close 列
        spy_series = safe_extract_close(spy_raw, "SPY")
        vix_series = safe_extract_close(vix_raw, "^VIX")

        # 3. 获取最近交易日、价格和 PE Ratio
        last_trading_day = spy_series.index[-1]
        curr_spy = spy_series.iloc[-1]
        curr_vix = vix_series.iloc[-1]
        curr_pe = get_latest_sp500_pe()

        print(f"最近交易日: {last_trading_day.strftime('%Y-%m-%d')}")
        print(f"SPY 收盘价: {curr_spy:.2f}")
        print(f"VIX 指数: {curr_vix:.2f}")
        if curr_pe:
            print(f"S&P 500 PE Ratio: {curr_pe:.2f}")
        else:
            print("无法获取 S&P 500 PE Ratio")

        # 4. 检查数据长度
        if len(spy_series) < 61:
            print("⚠️ 数据不足60个交易日")
            return

        # 5. 计算最近60个交易日（不含最后一个）的最高点
        recent_60 = spy_series.iloc[-61:-1]  # 倒数第61到倒数第2（共60个）
        max_spy = recent_60.max()
        pullback = (max_spy - curr_spy) / max_spy * 100

        print(f"近期高点: {max_spy:.2f}, 回撤幅度: {pullback:.2f}%")

        # 6. 条件判断
        #vix恐慌指数判断
        vixcon1 = (30 < curr_vix < 40)
        vixcon2 = (curr_vix >= 40)
        #spy500的回撤判断
        pullbackcon1 = (5 < pullback <= 10)
        pullbackcon2 = (10 < pullback <= 20)
        pullbackcon3 = (pullback > 20)
        #spy500的市盈率判断
        pecon1 = (curr_pe < 20)
        pecon2 = (20<=curr_pe <= 27)
        pecon3 = (curr_pe > 27)


        # 7. 输出策略
        print("\n--- 策略建议 ---")

        if vixcon2 and pullbackcon3 and pecon1:
            advice = "✅✅✅ 大量加仓"
            print("✅✅✅ 大量加仓")
        elif vixcon1 and pullbackcon2 and pecon1:
            advice ="✅✅ 适度加仓"
            print("✅✅ 适度加仓")
        elif vixcon2 and pullbackcon3 and pecon3:
            advice ="✅ 建议关注"
            print("✅ 建议关注")
        elif pecon3 and (vixcon1 or vixcon2) and pullbackcon1:
            advice ="主动减仓"
            print("主动减仓")
        else:
            advice ="当前不满足任何预设条件，建议持有"
            print("当前不满足任何预设条件，建议持有")
            #不需要通知
            return

        send_wechat_notification(
            title="📈 交易信号提醒",
            content=(
                f"**VIX恐慌指数异常！**\n\n"
                f"- 当前 VIX: {curr_vix:.2f}\n"
                f"- SPY 回撤: {pullback:.2f}%\n"
                f"- S&P 500 PE: {f'{curr_pe:.2f}' if curr_pe else '获取失败'}\n"
                f"- 建议操作: {advice}\n"
                f"> 最近交易日: {last_trading_day.strftime('%Y-%m-%d')}"
            ),
            send_key=SEND_KEY
        )

    except Exception as e:
        print(f"❌ 程序出错: {e}")

if __name__ == "__main__":
    main()