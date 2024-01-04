from django.contrib import admin

from .models import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork
    raw_id_fields = ("person",)


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (
        GenreFilmworkInline,
        PersonFilmworkInline,
    )
    list_display = ("title", "type", "creation_date", "rating", "created", "modified")
    list_filter = ("type", "rating")
    search_fields = ("title", "description", "id")


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ("name", "created", "modified")
    search_fields = ("name", "description", "id")


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ("full_name", "created", "modified")
    search_fields = ("full_name", "description", "id")
