FROM public.ecr.aws/lambda/python:3.14.2025.11.19.23

# Install git and git clone the geff repo
RUN dnf update -y && \
    dnf install -y git && \
    dnf clean all

RUN mkdir "${LAMBDA_TASK_ROOT}/geff/"

COPY lambda_src/ "${LAMBDA_TASK_ROOT}/geff/"

COPY requirements.txt  "${LAMBDA_TASK_ROOT}"

# Install the function's dependencies using file requirements.txt
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "geff.lambda_function.lambda_handler" ]
