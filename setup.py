"""Cython 扩展编译入口: df-msgpack 加速模块.

Windows:
    uv run python setup.py build_ext --inplace
Linux / macOS:
    uv run python setup.py build_ext --inplace

编译产物 (.pyd / .so) 放在 src/pulsemq/serialization/ 下, 与 .pyx 同级.
"""
from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np

extensions = [
    Extension(
        "pulsemq.serialization._df_msgpack",
        sources=["src/pulsemq/serialization/_df_msgpack.pyx"],
        include_dirs=[np.get_include()],
        define_macros=[("NPY_NO_DEPRECATED_API", "NPY_1_7_API_VERSION")],
    ),
]

setup(
    name="pulsemq_df_msgpack_cython",
    ext_modules=cythonize(extensions, language_level="3"),
    license="MIT",
)
