from jinja2 import Template


def render_jinja2_template(template_str: str, context: dict) -> str:
    """
    Renders a Jinja2 template with the given context.

    Args:
        template_str (str): The Jinja2 template string.
        context (dict): The context dictionary to render the template.

    Returns:
        str: The rendered template string.
    """
    template = Template(template_str)
    return template.render(context)
