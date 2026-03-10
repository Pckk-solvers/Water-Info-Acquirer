from __future__ import annotations

import inspect


def supports_input_arg(input_type: type, arg_name: str) -> bool:
    try:
        parameters = inspect.signature(input_type).parameters.values()
    except (TypeError, ValueError):
        return False
    names = {parameter.name for parameter in parameters}
    if arg_name in names:
        return True
    return any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters)
