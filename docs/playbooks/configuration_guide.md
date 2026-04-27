# Configuration Guide

## Purpose

配置文件的目标，是把“运行方式”和“可变规则入口”从代码里分离出来。

代码负责执行，配置负责选择。

## Current Layout

- `configs/default.yaml`
- `configs/conservative.yaml`

## What Is Configured Today

### fetch

- 是否允许抓取
- 是否启用黑名单
- 黑名单文件路径

### baseline

- 基础规则文件路径

### output

- 是否保存 CSV
- 保存到哪个路径

### sector_profiles

- 当前只记录行业手册引用
- 后续可扩展成真正的行业规则文件列表

## Current Boundary

当前这套配置还不是完整规则引擎。

它只解决：

1. 不改代码也能切换运行参数
2. 不改代码也能切换黑名单和输出文件
3. 后续加行业配置时有稳定入口

## Next Expansion

下一步适合继续配置化的内容：

1. 基础过滤阈值
2. 行业规则文件路径
3. 字母排序和导出行为
4. 候选池输出路径
