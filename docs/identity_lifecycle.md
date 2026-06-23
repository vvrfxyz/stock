# 证券身份生命周期

## 核心原则

`security_id` 是全系统的持久身份锚点。`symbol` 是可变属性——会因 ticker rename（如 FB→META）而变化，也可能被交易所回收给另一家公司（ticker recycle）。

## 身份解析器 (SecurityIdentityResolver)

位置：`utils/security_identity.py`

### 解析优先级

每条 incoming vendor 数据按以下顺序匹配到一个 `security_id`：

1. **FIGI 精确匹配**（最强信号，全球唯一）→ confidence=HIGH
2. **CIK 精确匹配**（SEC 层面唯一）→ confidence=HIGH（唯一命中）或 MEDIUM（交易所消歧）
3. **活跃 symbol 精确匹配** → confidence=HIGH（无冲突）或 LOW（FIGI/CIK 冲突=回收）
4. **历史 symbol 匹配**（`security_symbol_history`）→ confidence=MEDIUM
5. **无匹配** → 新上市，security_id=-1

### 处理流程

`sync_massive_universe` 每日执行时：

```
incoming tickers
      │
      ▼
SecurityIdentityResolver.resolve_batch()
      │
      ├─ FIGI/CIK 匹配 + symbol 变了 → RENAME
      │   ├─ rename_security(old→new)
      │   ├─ 写 symbol_history
      │   ├─ 写 identity_event(RENAME)
      │   └─ upsert_security_info 更新元数据
      │
      ├─ symbol 匹配但 FIGI/CIK 冲突 → RECYCLE
      │   ├─ 跳过 upsert
      │   └─ 写 identity_event(QUARANTINE)
      │
      └─ 正常匹配 / 新上市 → upsert_securities_by_symbol
```

### 内层防御

`upsert_securities_by_symbol` 仍保留自己的 FIGI/CIK 冲突检测作为兜底（`update_massive_details` 等脚本直接调用它，不经 resolver）。冲突事件写入 `security_identity_events` 表。

## 存量修复

对历史积累的身份分裂（同 FIGI 多个 security_id），使用修复工具：

```bash
# 1. 先审计
python scripts/audit_security_identity.py

# 2. 查看修复 plan（dry-run，不写库）
python scripts/repair_identity.py --dry-run

# 3. 确认无误后执行
python scripts/repair_identity.py --apply
```

合并逻辑：选择有更多数据的 id 保留，将其他 id 的数据行迁移过来，旧 id 标 inactive，写 MERGE 事件。

## 身份事件表 (security_identity_events)

| event_type | 含义 |
|-----------|------|
| RENAME | 同一身份，symbol 变了 |
| RECYCLE | 同一 symbol，不同身份（被新公司复用）|
| QUARANTINE | 冲突无法自动解决，跳过并记录 |
| MERGE | 人工/审计合并分裂身份 |
| NEW_LISTING | 新上市 |
| MANUAL | 人工修正 |

## 监控

- `audit_security_identity` 每周日自动运行（`scheduled_update` 步骤）
- `health_report` 展示身份健康摘要（同 FIGI 多 id、近 30 天 identity events）
- 退出码：0=无问题，1=advisory（需关注），2=blocking（阻塞部署）
