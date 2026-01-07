{% macro generate_yaml(directory_name, prefix_text='') %}

    {# Lấy danh sách models dựa trên tham số truyền vào #}
    {% set models_to_generate = codegen.get_models(directory=directory_name, prefix=prefix_text) %}

    {# Sinh ra YAML #}
    {{ codegen.generate_model_yaml(
        model_names = models_to_generate
    ) }}

{% endmacro %}