import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="aws_emr_spark",
    version="0.0.1",

    description="A CDK App to deploy an AWS EMR Spark cluster",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="Matthieu Lienart",

    package_dir={"": "aws_emr_spark"},
    packages=setuptools.find_packages(where="aws_emr_spark"),

    install_requires=[
        "aws-cdk.core==1.112.0",
    ],

    python_requires=">=3.8",

    classifiers=[
        "Development Status :: 4 - Beta",

        "Intended Audience :: Developers",

        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",

        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",

        "Typing :: Typed",
    ],
)
