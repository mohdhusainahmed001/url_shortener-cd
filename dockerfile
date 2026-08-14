FROM ubuntu:latest


COPY _build/default/rel/url_shortener /work/url_shortener/

COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

WORKDIR /work/url_shortener/

ENTRYPOINT ["/entrypoint.sh"]

CMD ["foreground"]


#COPY set_args.sh /work/api/

#RUN chmod +x /work/api/set_args.sh

#ENTRYPOINT ["./set_args.sh"]
