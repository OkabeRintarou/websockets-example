# GitHub Actions 工作流

本项目包含多个 GitHub Actions 工作流，用于自动化测试 WebSocket 服务器和客户端。

## 工作流列表

### 1. WebSocket Server & Clients Test (`test-websocket.yml`)

**触发方式:**
- 推送到 `main`, `master`, `dev` 分支
- 对这些分支的 Pull Request
- 手动触发 (workflow_dispatch)

**功能:**
- 在多个 Python 版本 (3.9, 3.10, 3.11) 上测试
- 启动 1 个 WebSocket Server
- 启动 N 个 Client (默认 3 个，可手动指定)
- 验证所有进程正常运行
- 自动清理进程

**手动触发参数:**
- `client_count`: 客户端数量 (默认: 3)

**使用方法:**
```bash
# 自动触发 (push 或 PR)
git push origin main

# 手动触发
# 1. 进入 GitHub 仓库页面
# 2. 点击 "Actions" 标签
# 3. 选择 "WebSocket Server & Clients Test"
# 4. 点击 "Run workflow"
# 5. 输入客户端数量
```

### 2. Custom Client Count Test (`test-with-custom-clients.yml`)

**触发方式:**
- 仅支持手动触发 (workflow_dispatch)

**功能:**
- 启动 1 个 WebSocket Server
- 启动指定数量的 Client (1, 2, 3, 5, 10)
- 使用指定的负载均衡策略
- 持续监控 20 秒
- 显示详细的运行状态
- 生成测试摘要

**手动触发参数:**
- `client_count`: 客户端数量 (选项: 1, 2, 3, 5, 10)
- `load_balancing_strategy`: 负载均衡策略
  - `least_loaded`: 负载最小优先
  - `round_robin`: 轮询
  - `weighted_random`: 加权随机
  - `random`: 随机

**使用方法:**
```bash
# 1. 进入 GitHub 仓库页面
# 2. 点击 "Actions" 标签
# 3. 选择 "Custom Client Count Test"
# 4. 点击 "Run workflow"
# 5. 选择:
#    - 客户端数量 (如: 5)
#    - 负载均衡策略 (如: least_loaded)
# 6. 点击 "Run workflow" 确认
```

## 工作流特性

### ✅ 自动化测试
- 自动安装依赖 (websockets)
- 自动启动 server 和多个 clients
- 自动验证进程状态
- 自动清理所有进程

### 📊 监控与报告
- 实时显示进程启动状态
- 周期性检查进程健康状态
- 生成测试摘要 (GitHub Summary)
- 显示详细的日志信息

### 🛡️ 错误处理
- 超时保护 (10 分钟)
- 进程异常检测
- 自动清理残留进程 (always cleanup)
- 错误信息详细输出

### 🔧 灵活配置
- 可自定义客户端数量
- 可选择负载均衡策略
- 支持多 Python 版本测试
- 支持手动和自动触发

## 测试流程

### 标准测试流程 (test-websocket.yml)
```
1. Checkout 代码
2. 设置 Python 环境
3. 安装依赖 (websockets)
4. 启动 Server (后台运行)
5. 启动 N 个 Clients (后台运行)
6. 等待连接建立 (5 秒)
7. 验证所有进程运行正常
8. 清理所有进程
```

### 自定义测试流程 (test-with-custom-clients.yml)
```
1. Checkout 代码
2. 设置 Python 3.11
3. 安装依赖
4. 创建编排脚本 (orchestrator.py)
5. 修改 server.py 负载均衡策略
6. 启动 Server
7. 启动 N 个 Clients
8. 监控 20 秒 (每 5 秒检查一次状态)
9. 生成测试摘要
10. 清理所有进程
```

## 监控输出示例

```
🚀 启动 WebSocket Server...
   负载均衡策略: least_loaded
  ✅ Server PID: 1234

🚀 启动 5 个 Clients...
  ✅ Client #1 PID: 1235
  ✅ Client #2 PID: 1236
  ✅ Client #3 PID: 1237
  ✅ Client #4 PID: 1238
  ✅ Client #5 PID: 1239

⏳ 等待连接建立...

⏱️  监控运行状态 (20 秒)...
  📊 Server: 运行中 | Clients: 5/5 活跃
  📊 Server: 运行中 | Clients: 5/5 活跃
  📊 Server: 运行中 | Clients: 5/5 活跃
  📊 Server: 运行中 | Clients: 5/5 活跃

✅ 测试成功完成

🧹 清理所有进程...
  停止 Client #1
  停止 Client #2
  停止 Client #3
  停止 Client #4
  停止 Client #5
  停止 Server
✅ 清理完成
```

## 故障排查

### 问题: Server 启动失败
**解决方案:**
- 检查端口 8765 是否被占用
- 查看 Server 日志输出
- 确认 server.py 语法正确

### 问题: Client 无法连接
**解决方案:**
- 增加等待时间 (修改 sleep 时长)
- 检查网络配置
- 确认 Server 已正常启动

### 问题: 进程未清理
**解决方案:**
- 使用 `if: always()` 确保清理步骤执行
- 添加 `kill -9` 强制终止
- 增加清理等待时间

## 最佳实践

1. **测试前提交代码**: 确保代码已提交到仓库
2. **选择合适的客户端数量**: 根据测试目的选择 1-10 个
3. **查看完整日志**: 展开所有步骤查看详细输出
4. **定期运行**: 在代码变更后运行完整测试
5. **监控资源**: 注意 GitHub Actions 的使用配额

## 配额说明

GitHub Actions 免费配额:
- **Public 仓库**: 无限制
- **Private 仓库**: 2000 分钟/月 (免费账户)

每次运行时间:
- 标准测试: ~3-5 分钟
- 自定义测试: ~5-8 分钟

## 扩展建议

### 添加更多测试场景
```yaml
- name: Test error recovery
  run: |
    python test_error_decay.py

- name: Test load balancing
  run: |
    python test_example.py
```

### 添加性能测试
```yaml
- name: Performance test
  run: |
    # 运行 100 次请求
    for i in {1..100}; do
      # 发送测试请求
    done
```

### 添加通知
```yaml
- name: Notify on failure
  if: failure()
  uses: actions/slack-notify@v1
  with:
    webhook_url: ${{ secrets.SLACK_WEBHOOK }}
```

## 总结

这些 GitHub Actions 工作流提供了:
- ✅ 自动化测试流程
- ✅ 多场景测试覆盖
- ✅ 灵活的配置选项
- ✅ 详细的监控和报告
- ✅ 可靠的错误处理

通过这些工作流，您可以确保 WebSocket 服务器和客户端在各种配置下都能正常工作。
