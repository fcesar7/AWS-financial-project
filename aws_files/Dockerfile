FROM public.ecr.aws/lambda/python:3.12

# Copy requirements.txt
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip install -r requirements.txt

# Copy all files in /source
COPY ./* ${LAMBDA_TASK_ROOT}

# set the CMD to your handler
CMD [ "model_lambda.lambda_handler" ]