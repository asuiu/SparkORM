from sparkorm import Struct, String, Integer, Decimal
import decimal


class User(Struct):
    id = Integer(name="user_id", nullable=False)
    username = String()


class Article(Struct):
    id = Integer(name="article_id", nullable=False)
    title = String()
    author = User()
    text = String(name="body")
    print_price_usd = Decimal(1000, 2)


article_a = Article.make_dict(
    id=1001,
    title="The article title",
    author=User.make_dict(
        id=440,
        username="user"
    ),
    text="Lorem ipsum article text lorem ipsum."
)

article_a_as_dict = {
    "article_id": 1001,
    "author": {
        "user_id": 440,
        "username": "user"},
    "body": "Lorem ipsum article text lorem ipsum.",
    "title": "The article title",
    "print_price_usd": None
}


article_b = Article.make_dict(
    id=1002
)

article_b_as_dict = {
    "article_id": 1002,
    "author": None,
    "body": None,
    "title": None,
    "print_price_usd": None
}


article_c = Article.make_dict(
    id=1003,
    print_price_usd=decimal.Context(prec=3).create_decimal('1.99')
)

article_c_as_dict = {
    "article_id": 1003,
    "author": None,
    "body": None,
    "title": None,
    "print_price_usd": decimal.Context(prec=3).create_decimal('1.99')
}
