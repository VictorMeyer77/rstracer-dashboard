import io
import os

from setuptools import find_packages, setup


def read(*paths, **kwargs):
    with io.open(
        os.path.join(os.path.dirname(__file__), *paths),
        encoding=kwargs.get("encoding", "utf8"),
    ) as open_file:
        content = open_file.read().strip()
    return content


def read_requirements(path):
    return [line.strip() for line in read(path).split("\n") if not line.startswith(('"', "#", "-", "git+"))]


setup(
    name="rstracer-dashboard",
    version=read("VERSION"),
    description="Explore your rstracer database !",
    url="https://github.com/VictorMeyer77/rstracer-dashboard",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    author="Victor Meyer",
    packages=find_packages(exclude=[".github"]),
    install_requires=read_requirements("requirements.txt"),
    extras_require={"test": read_requirements("requirements-dev.txt")},
    project_urls=(
        {
            "Bug Tracker": "https://github.com/VictorMeyer77/rstracer-dashboard/issues",
            "Source": "https://github.com/VictorMeyer77/rstracer-dashboard",
        }
    ),
)
