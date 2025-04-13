from setuptools import setup, find_packages

setup(
    name="fogverse",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "aiokafka",
        "confluent-kafka",
        "opencv-python",
        "numpy",
        "kubernetes",
    ],
    python_requires=">=3.7",
)
