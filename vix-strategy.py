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


def main():

    SEND_KEY = "SCT312240T75M1tG903ZKOzaKdA42lgr8n"
    
    try:
        # 1. 直接下载最近90个交易日数据（避免手动日期计算）
        spy_raw = yf.download("SPY", period="90d", progress=False)
        vix_raw = yf.download("^VIX", period="90d", progress=False)

        # 2. 安全提取 Close 列
        spy_series = safe_extract_close(spy_raw, "SPY")
        vix_series = safe_extract_close(vix_raw, "^VIX")

        # 3. 获取最近交易日和价格
        last_trading_day = spy_series.index[-1]
        curr_spy = spy_series.iloc[-1]
        curr_vix = vix_series.iloc[-1]

        print(f"最近交易日: {last_trading_day.strftime('%Y-%m-%d')}")
        print(f"SPY 收盘价: {curr_spy:.2f}")
        print(f"VIX 指数: {curr_vix:.2f}")

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
        vixcon1 = (30 < curr_vix < 40)
        vixcon2 = (curr_vix >= 40)
        spycon1 = (5 < pullback <= 10)
        spycon2 = (10 < pullback <= 20)
        spycon3 = (pullback > 20)

        # 7. 输出策略
        print("\n--- 策略建议 ---")
        if (vixcon1 and spycon1) or (vixcon2 and spycon1):
            advice ="✅建议关注"
            print("✅ 关注")
        elif (vixcon1 and spycon2) or (vixcon1 and spycon3) or (vixcon2 and spycon2):
            advice ="✅✅适度加仓"
            print("✅ 适度加仓")
        elif vixcon2 and spycon3:
            advice ="✅✅✅ 大量加仓"
            print("✅✅✅ 大量加仓")
        else:
            advice ="当前不满足任何预设条件（VIX 或回撤未达阈值）"
            print("ℹ️ 当前不满足任何预设条件（VIX 或回撤未达阈值）")
            #不需要通知
            #return

        send_wechat_notification(
            title="📈 交易信号提醒",
            content=(
                f"**VIX恐慌指数异常！**\n\n"
                f"- 当前 VIX: {curr_vix:.2f}\n"
                f"- SPY 回撤: {pullback:.2f}%\n"
                f"- 建议操作: {advice}\n"
                f"> 最近交易日: {last_trading_day.strftime('%Y-%m-%d')}"
            ),
            send_key=SEND_KEY
        )

    except Exception as e:
        print(f"❌ 程序出错: {e}")

if __name__ == "__main__":
    main()