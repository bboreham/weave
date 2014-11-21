FROM scratch
WORKDIR /home/weave
ADD ./weaveconf /home/weave/
EXPOSE 6787/tcp
ENTRYPOINT ["/home/weave/weaveconf", "-wait", "5", "-iface", "ethwe"]
