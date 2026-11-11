from setuptools import find_packages, setup

setup(
    name="wget2",
    version="1.4.7",
    packages=find_packages(
        where="src",
    ),
    entry_points={
        "console_scripts": [
            "wget2=wget2.cli:main",
        ],
    },
    author="Isaac Onagh",
    description="Multi-threaded Python-based wget alternative",
    url="https://github.com/i4147/wget2",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.11",
)
