"""GitHub Copilot CLI 后端实现。

通过子进程调用 `copilot -p "<prompt>" --yolo` 实现非交互式 Agent 调用。

关键参数说明：
- `-p`：直接传入 prompt，跳过交互式输入
- `--yolo`：等价于 `--allow-all-tools --allow-all-paths`，允许 Agent 自由操作
- `--add-dir`：限制 Agent 可访问的目录范围
- `--effort`：推理强度（high / medium / low），影响 Agent 的思考深度

环境要求：
- 需要安装 Copilot CLI：
  npm install -g @githubnext/github-copilot-cli
  或通过 GitHub CLI 扩展安装
- 需要已完成 GitHub 认证（`copilot auth`）
"""

from __future__ import annotations

import shutil
import subprocess

from vortex.agent.backend import AgentResult


class CopilotBackend:
    """GitHub Copilot CLI 后端。

    调用方式：
        copilot -p "<prompt>" --yolo [--add-dir <scope>] [--effort <level>]

    如果 copilot 二进制不存在，is_available() 返回 False，
    invoke() 会返回一个 success=False 的 AgentResult 而不是抛异常。
    """

    # Agent 调用的默认超时时间（秒）。
    # 复杂任务可能需要较长时间，但也不能无限等待。
    DEFAULT_TIMEOUT = 600

    def __init__(self) -> None:
        self._binary: str | None = self._find_binary()

    @property
    def name(self) -> str:
        return "copilot"

    def is_available(self) -> bool:
        """检查 copilot 命令是否存在于 PATH 中。"""
        return self._binary is not None

    def invoke(
        self,
        prompt: str,
        *,
        scope: str = "",
        effort: str = "high",
    ) -> AgentResult:
        """调用 Copilot CLI 执行任务。

        Args:
            prompt: 发送给 Copilot 的任务描述
            scope: 工作目录范围（传给 --add-dir）
            effort: 推理强度（传给 --effort）

        Returns:
            AgentResult，包含输出文本和退出码
        """
        if not self._binary:
            return AgentResult(
                success=False,
                output="copilot CLI 未安装，请先安装后再使用。\n" + self.install_hint(),
                exit_code=-1,
                backend=self.name,
            )

        cmd = [self._binary, "-p", prompt, "--yolo"]
        if scope:
            cmd.extend(["--add-dir", scope])
        if effort:
            cmd.extend(["--effort", effort])

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.DEFAULT_TIMEOUT,
            )
            return AgentResult(
                success=result.returncode == 0,
                output=(result.stdout or "") + (result.stderr or ""),
                exit_code=result.returncode,
                backend=self.name,
            )
        except subprocess.TimeoutExpired:
            return AgentResult(
                success=False,
                output=f"copilot 调用超时（{self.DEFAULT_TIMEOUT}s）",
                exit_code=-2,
                backend=self.name,
            )
        except OSError as exc:
            return AgentResult(
                success=False,
                output=f"copilot 启动失败: {exc}",
                exit_code=-3,
                backend=self.name,
            )

    def install_hint(self) -> str:
        """返回 Copilot CLI 的安装指引。"""
        return (
            "安装 GitHub Copilot CLI：\n"
            "  npm install -g @githubnext/github-copilot-cli\n"
            "\n"
            "安装后需要完成认证：\n"
            "  copilot auth\n"
            "\n"
            "详细文档：https://docs.github.com/en/copilot/using-github-copilot/using-github-copilot-in-the-command-line"
        )

    @staticmethod
    def _find_binary() -> str | None:
        """在 PATH 中查找 copilot 命令。"""
        return shutil.which("copilot")
