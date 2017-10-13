FROM bryanyang0528/centos-git-python3
MAINTAINER Bryan Yang <bryan.yang@vpom.com>
USER root
WORKDIR /workspace
ADD . /workspace
RUN pip install pip setuptools --upgrade
RUN pip install -r requirements.txt

CMD ["/bin/bash"]
