# LivePositionView

##
1. 从各个Oms.db 获取position，间隔5min
2. 从QMReport.db 获取initX，间隔60min
3. 从MSServer 获取Signal标准Position
4. 分Trader 计算PerInitX TickerHoldingPosition
5. 画图
