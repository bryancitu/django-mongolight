# Django-MongoLight

A library light for integrate Django with MongoDB

## Instalation

```bash
pip install django-mongolight
```

* Use

1. Configura tu base de datos en settings.py:

```
DATABASES = {
    'default': {
        'ENGINE': 'mongolight',
        'NAME': 'mydatabase',
        'HOST': 'localhost',
        'PORT': 27017,
    }
}
```

2.Create models:

```
from django.db import models
from mongolight.fields import ObjectIdField

class User(models.Model):
    _id = ObjectIdField(primary_key=True)
    name = models.CharField(max_length=100)
```