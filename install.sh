#!/bin/bash
# CME 数据下载器安装脚本
# 适用于 macOS (Apple Silicon)

set -e

echo "======================================"
echo "CME 数据下载器 - 安装脚本"
echo "======================================"

# 检查 Python 版本
echo ""
echo "1. 检查 Python 环境..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
    echo "✓ 找到 Python: $PYTHON_VERSION"
else
    echo "✗ 未找到 Python 3，请先安装 Python 3.8 或更高版本"
    echo "  推荐使用 Homebrew 安装: brew install python3"
    exit 1
fi

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# 创建虚拟环境（可选但推荐）
echo ""
echo "2. 创建虚拟环境..."
if [ -d "venv" ]; then
    echo "  虚拟环境已存在，跳过创建"
else
    python3 -m venv venv
    echo "✓ 虚拟环境创建成功"
fi

# 激活虚拟环境
source venv/bin/activate

# 安装依赖
echo ""
echo "3. 安装依赖包..."
pip install --upgrade pip
pip install -r requirements.txt
echo "✓ 依赖包安装完成"

# 创建必要的目录
echo ""
echo "4. 创建必要目录..."
mkdir -p logs data
echo "✓ 目录创建完成"

# 设置执行权限
echo ""
echo "5. 设置脚本执行权限..."
chmod +x run.sh
chmod +x main.py
echo "✓ 权限设置完成"

# 验证配置
echo ""
echo "6. 验证配置..."
python main.py --validate
if [ $? -eq 0 ]; then
    echo "✓ 配置验证通过"
else
    echo "✗ 配置验证失败，请检查 src/config.py"
    exit 1
fi

# 测试运行
echo ""
echo "7. 测试运行（仅解析链接）..."
python main.py --test
if [ $? -eq 0 ]; then
    echo "✓ 测试运行成功"
else
    echo "✗ 测试运行失败"
    exit 1
fi

echo ""
echo "======================================"
echo "安装完成！"
echo "======================================"
echo ""
echo "下一步操作："
echo "1. 修改配置文件（如需要）："
echo "   nano src/config.py"
echo ""
echo "2. 手动测试下载："
echo "   python main.py"
echo ""
echo "3. 配置定时任务："
echo "   请查看 README.md 中的 'Crontab 配置' 部分"
echo ""
echo "4. 配置 macOS 权限："
echo "   请查看 README.md 中的 'macOS 权限设置' 部分"
echo ""
