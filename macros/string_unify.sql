{% macro string_unify_no_punctuation(text) %}REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD({{text}}, NFD),"[\u0300-\u036f]",""){% endmacro %}

{% macro string_unify_no_special_characters(text,filter = "a-z0-9") %}REGEXP_REPLACE(REGEXP_REPLACE({{text}},"[^{{filter}}]"," "),r"\\s+"," "){% endmacro %}

{% macro string_unify(text) %}{{string_unify_no_special_characters(string_unify_no_punctuation(text),"a-z0-9|")}}{% endmacro %}
