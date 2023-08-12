from examples.struct_instantiation.instantiate_as_dict import (
    article_a, article_a_as_dict,
    article_b, article_b_as_dict,
    article_c, article_c_as_dict
)


def test_article_a():
    assert article_a == article_a_as_dict


def test_article_b():
    assert article_b == article_b_as_dict


def test_article_c():
    assert article_c == article_c_as_dict
