from setuptools import setup

setup(
    name="faf_replay_server",
    version='0.0.1',
    description="FAF replay server using asyncio",
    long_description="TODO",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
    ],
    author="Igor Kotrasiński",
    author_email="i.kotrasinsk@gmail.com",
    url="https://github.com/FAForever/faf-aio-replayserver",
    keywords="FAForever replay server",
    license="GPL3",
    packages=["replayserver"],
    entry_points={
        "console_scripts": [
            "faf_replay_server = replayserver.main:main",
        ],
    install_requires=[
        "aiomysql",
        ],
    },
)
