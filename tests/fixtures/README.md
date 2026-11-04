# Fixture

`recentchange_sample.jsonl` : 220 événements bruts du flux
`https://stream.wikimedia.org/v2/stream/recentchange`, enregistrés le 4 novembre 2026
vers 10 h 25 UTC avec `python -m wikistream record-fixture --n 220`. Seul le champ
`parsedcomment` (copie HTML de `comment`) a été retiré pour tenir sous 300 Ko.

Source : Wikimedia EventStreams. Les métadonnées d'événements sont publiées sous
CC0 ; les éléments textuels qu'elles reprennent (titres de pages, résumés de
modification, noms d'utilisateurs) viennent des wikis d'origine, sous licence
CC BY-SA 4.0. Voir https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service
et https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use.
