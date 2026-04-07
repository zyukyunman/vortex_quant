"""配置合并器：把多来源配置折叠成一个最终 dict。

这里的关键词是“优先级”：

    defaults < parent < user < overrides

也就是说，越靠右的来源越“强”，后写入的值会覆盖前面的值。

注意：这里只做字典层面的合并，不做类型校验、不做业务语义判断。
"""
from __future__ import annotations

import copy


class ProfileMerger:
    """合并 defaults + parent + user + CLI override。"""

    def merge(
        self,
        defaults: dict,
        parent: dict | None,
        user: dict,
        overrides: dict | None = None,
    ) -> dict:
        """按优先级合并配置。

        对于 dict 类型的值做递归浅合并，其余类型直接覆盖。
        """
        # 第一步永远从默认值模板起步，保证“未显式填写的字段也有兜底”。
        result = copy.deepcopy(defaults)
        # 第二步应用父配置，让子配置可以继承一份基础模板。
        if parent:
            result = self._deep_merge(result, parent)
        # 第三步应用用户自己的 YAML，这通常是最主要的显式配置来源。
        result = self._deep_merge(result, user)
        # 最后再应用临时 override，例如 CLI 上用 `--set` 传入的值。
        if overrides:
            result = self._deep_merge(result, overrides)
        return result

    @staticmethod
    def _deep_merge(base: dict, overlay: dict) -> dict:
        """递归合并 overlay 到 base，overlay 优先。

        合并规则：

        - 如果两边同名字段都是 dict：继续往下一层递归合并
        - 其他类型（标量、list、None 等）：直接用 overlay 覆盖 base
        """
        merged = copy.deepcopy(base)
        for key, value in overlay.items():
            if (
                key in merged
                and isinstance(merged[key], dict)
                and isinstance(value, dict)
            ):
                # 两边都是字典时，保留已有结构并递归补丁式合并。
                merged[key] = ProfileMerger._deep_merge(merged[key], value)
            else:
                # 非 dict 值直接覆盖，例如 list、字符串、数字、None。
                merged[key] = copy.deepcopy(value)
        return merged
