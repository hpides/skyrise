import enum
import termcolor


class InfoType(enum.Enum):
    DOUBLE_SEPARATOR = 1
    SINGLE_SEPARATOR = 2
    RUN = 3
    PLOT = 4
    SKIPPED = 5
    PASSED = 6
    FAILED = 7


info_type_texts_and_colors = {
    InfoType.DOUBLE_SEPARATOR: ("[==========]", "green"),
    InfoType.SINGLE_SEPARATOR: ("[----------]", "green"),
    InfoType.RUN: ("[ RUN      ]", "green"),
    InfoType.PLOT: ("[ PLOT     ]", "green"),
    InfoType.SKIPPED: ("[  SKIPPED ]", "yellow"),
    InfoType.PASSED: ("[  PASSED  ]", "green"),
    InfoType.FAILED: ("[  FAILED  ]", "red"),
}


def print_info(info_type: InfoType, info: str = ""):
    info_text, info_color = info_type_texts_and_colors[info_type]
    print(f"{termcolor.colored(info_text, info_color)} {info}")
