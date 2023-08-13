from sparkorm import String, Array, Struct
from sparkorm.models import TableModel


class Address(Struct):
    post_code = String()
    city = String()


class User(Struct):
    username = String(nullable=False)
    address = Address()


class Comment(Struct):
    message = String()
    author = User(nullable=False)


class Article(TableModel):
    title = String(nullable=False)
    author = User(nullable=False)
    comments = Array(Comment())


author_city_str = Article.author.address.city.PATH
"author.address.city"

comment_usernames_str = Article.comments.e.author.username.PATH
"comments.author.username"

comment_usernames_str = Article.comments.author.username.PATH
"comments.author.username"

comment_messages_str = Article.comments.message.PATH
"comments.message"

author_city_col = Article.author.address.city.COL
comment_usernames_col = Article.comments.e.author.username.COL
comment_messages_col = Article.comments.message.COL
