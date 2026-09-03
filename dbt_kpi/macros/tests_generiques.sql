{#
  Tests génériques maison.
  Écrits ici plutôt qu'importés de dbt_utils : le projet n'a alors aucune
  dépendance externe, et les règles restent lisibles dans le dépôt.
#}

{% test intervalle_accepte(model, column_name, min_value=none, max_value=none, inclusive=true) %}
    {%- set op_min = '<' if inclusive else '<=' -%}
    {%- set op_max = '>' if inclusive else '>=' -%}
    select {{ column_name }} as valeur_hors_intervalle
    from {{ model }}
    where {{ column_name }} is not null
      and (
        {% if min_value is not none %} {{ column_name }} {{ op_min }} {{ min_value }} {% else %} false {% endif %}
        or
        {% if max_value is not none %} {{ column_name }} {{ op_max }} {{ max_value }} {% else %} false {% endif %}
      )
{% endtest %}

{% test combinaison_unique(model, colonnes) %}
    select {{ colonnes | join(', ') }}, count(*) as nb
    from {{ model }}
    group by {{ range(1, colonnes | length + 1) | join(', ') }}
    having count(*) > 1
{% endtest %}
