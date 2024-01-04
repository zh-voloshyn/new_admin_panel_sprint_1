import uuid

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(_("created"), auto_now_add=True)
    modified = models.DateTimeField(_("modified"), auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_("name"), max_length=255)
    description = models.TextField(_("description"), blank=True)

    class Meta:
        db_table = 'content"."genre'
        verbose_name = "Жанр"
        verbose_name_plural = "Жанры"

    def __str__(self):
        return self.name


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_("full_name"), max_length=255)

    class Meta:
        db_table = 'content"."person'
        verbose_name = "Участник фильма"
        verbose_name_plural = "Участники фильмов"

    def __str__(self):
        return self.full_name


class Filmwork(UUIDMixin, TimeStampedMixin):
    class Type(models.TextChoices):
        movie = "movie", _("movie")
        tv_show = "tv_show", _("tv_show")

    title = models.CharField(_("title"), max_length=255)
    description = models.TextField(_("description"), blank=True)
    creation_date = models.DateField(_("creation_date"))
    rating = models.FloatField(_("rating"), blank=True, validators=[MinValueValidator(0), MaxValueValidator(100)])
    type = models.CharField(_("type"), max_length=20, choices=Type.choices, default=Type.movie)

    class Meta:
        db_table = 'content"."film_work'
        verbose_name = "Кинопроизведение"
        verbose_name_plural = "Кинопроизведения"

    def __str__(self):
        return self.title

    genres = models.ManyToManyField(Genre, through="GenreFilmwork")
    persons = models.ManyToManyField(Person, through="PersonFilmwork")


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey("Filmwork", on_delete=models.CASCADE)
    genre = models.ForeignKey("Genre", on_delete=models.CASCADE, verbose_name="Жанр кинопроизведения")
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'content"."genre_film_work'
        verbose_name_plural = "Жанры кинопроизведения"


class PersonFilmwork(UUIDMixin):
    person = models.ForeignKey("Person", on_delete=models.CASCADE, verbose_name="Участник кинопроизведения")
    film_work = models.ForeignKey("Filmwork", on_delete=models.CASCADE)
    role = models.TextField(_("role"))
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'content"."person_film_work'
        verbose_name_plural = "Участники кинопроизведения"
