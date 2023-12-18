from django import forms
from django.db import models
from django.contrib import admin, messages
from django.forms import ModelForm, PasswordInput
from celery import current_app
from django.contrib.auth.models import Group
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, redirect
from django.template import context, loader
from django.core.exceptions import ValidationError
from django import forms
from django.forms import inlineformset_factory
from django.utils.html import format_html
from django.contrib.sites.models import Site
from django.forms.widgets import Select
from celery.utils import cached_property
from django_celery_beat.admin import (
    PeriodicTaskAdmin as BasePeriodicTaskAdmin,
    PeriodicTaskForm as BasePeriodicTaskForm,
)
from dbt.users.models import User
from django.forms import ModelForm, Textarea
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_protect
from django.utils.safestring import mark_safe
from django import template
register = template.Library()

csrf_protect_m = method_decorator(csrf_protect)



from dbt.analytics.models import (
    DBTLogs,
    PythonLogs,
    GitRepo,
    ProfileYAML,
    SubProcessLog,
    PeriodicTask
    # Variant
)
from dbt.utils.common import clone_git_repo
from urllib import request






class GitRepoForm(ModelForm):
    url = forms.CharField(widget=PasswordInput())

    class Meta:
        model = GitRepo
        fields = "__all__"

        widgets = {
            'url': Textarea(attrs={'cols': 130, 'rows': 20}),
        }


# @admin.display
# def colored_name(self):
#         return format_html(
#             '<span style="color: #{};">{} {}</span>',
#             self.color_code,
#             self.first_name,
#             self.last_name,
#         )


# class PersonAdmin(admin.ModelAdmin):
#     list_display = ["first_name", "last_name", "colored_name"]

# class VariantForm(forms.ModelForm):

#     def validate_price(value):
#        if value > 1000:
#          msg = 'Price must be less than or equal to 1000'
#          raise ValidationError(msg)

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.fields['price'].validators.append(validate_price)

#         for visible in self.visible_fields():
#             visible.field.widget.attrs['class'] = 'form-control'
#             visible.field.widget.attrs['placeholder'] = visible.field.label

#     class Meta:
#         model = Variant
#         exclude = ('GitRepo', )

@admin.register(DBTLogs)
class DBTLogsLAdmin(admin.ModelAdmin):
    list_display = [
        "created_at",
        "completed_at",
        "success",
        "repository_used_name",
        "command",
        # "previous_command",
        "periodic_task_name",
        "profile_yml_used_name",
    ]
    readonly_fields = [
        "repository_used_name",
        "periodic_task_name",
        "profile_yml_used_name",
    ]

    search_fields = ("periodic_task_name", "created_at")
    list_filter = ("periodic_task_name", "created_at")

    # # change_form_template = 'admin/analytics/change_form.html'
    # # change_list_template = 'admin/analytics/change_list.html'

    # # def change_view(self, request, object_id, form_url="", extra_context=None):
    # #     extra_context = extra_context or {}
    # #     post = PythonLogs.objects.get(id=object_id)
    # #     extra_context["form"] = self.get_form(instance=post, request=request)
    # #     return super(PythonLogsLAdmin, self).change_view(request, object_id, form_url=form_url, extra_context=extra_context)

    def add_view(self, request, form_url="", extra_context=None):
        change_form_template = 'admin/analytics/change_form.html'
        extra_context = extra_context or {}
        # extra_context['form'] = self.get_form(request)
        return super(DBTLogsLAdmin, self).add_view(request, extra_context=extra_context)

    
    def changelist_view(self, request, extra_context=None):
        change_list_template = 'admin/analytics/change_list.html'
        extra_context = {'title': 'Select DBT logs to Change'}
        extra_context = extra_context or {}
        created_at = DBTLogs.objects.all().values_list('created_at', flat=True).distinct()
        completed_at = DBTLogs.objects.all().values_list('completed_at', flat=True).distinct()
        success = DBTLogs.objects.all().values_list('success', flat=True).distinct()
        repository_used_name  = DBTLogs.objects.all().values_list('repository_used_name', flat=True).distinct()
        command  = DBTLogs.objects.all().values_list('command', flat=True).distinct()
        # previous_command  = DBTLogs.objects.all().values_list('previous_command', flat=True).distinct()
        periodic_task_name = DBTLogs.objects.all().values_list('periodic_task_name', flat=True).distinct()
        profile_yml_used_name  = DBTLogs.objects.all().values_list('profile_yml_used_name', flat=True).distinct()
        extra_context.update({
            "created_at": created_at,
            "completed_at": completed_at,
            "success": success,
            "repository_used_name": repository_used_name,
            "command": command,
            # "previous_command":previous_command,
            "periodic_task_name":periodic_task_name,
            "profile_yml_used_name":profile_yml_used_name

        })
        return super().changelist_view(request, extra_context=extra_context)




# @admin.site.register(Variant)
# 
@admin.register(PythonLogs)
class PythonLogsLAdmin(admin.ModelAdmin):
    list_display = [
        "created_at",
        "completed_at",
        "success",
        "repository_used_name",
        "command",
        # "previous_command",
        "periodic_task_name",
        "profile_yml_used_name",
    ]
    readonly_fields = [
        "repository_used_name",
        "periodic_task_name",
        "profile_yml_used_name",
    ]

    search_fields = ("periodic_task_name", "created_at")
    list_filter = ("periodic_task_name", "created_at")

    # # change_form_template = 'admin/analytics/change_form.html'
    # # change_list_template = 'admin/analytics/change_list.html'

    # # def change_view(self, request, object_id, form_url="", extra_context=None):
    # #     extra_context = extra_context or {}
    # #     post = PythonLogs.objects.get(id=object_id)
    # #     extra_context["form"] = self.get_form(instance=post, request=request)
    # #     return super(PythonLogsLAdmin, self).change_view(request, object_id, form_url=form_url, extra_context=extra_context)

    def add_view(self, request, form_url="", extra_context=None):
        change_form_template = 'admin/analytics/change_form.html'
        extra_context = extra_context or {}
        # extra_context['form'] = self.get_form(request)
        return super(PythonLogsLAdmin, self).add_view(request, extra_context=extra_context)

    
    def changelist_view(self, request, extra_context=None):
        change_list_template = 'admin/analytics/change_list.html'
        extra_context = {'title': 'Select Python logs to Change'}
        extra_context = extra_context or {}
        created_at = PythonLogs.objects.all().values_list('created_at', flat=True).distinct()
        completed_at = PythonLogs.objects.all().values_list('completed_at', flat=True).distinct()
        success = PythonLogs.objects.all().values_list('success', flat=True).distinct()
        repository_used_name  = PythonLogs.objects.all().values_list('repository_used_name', flat=True).distinct()
        command  = PythonLogs.objects.all().values_list('command', flat=True).distinct()
        # previous_command  = PythonLogs.objects.all().values_list('previous_command', flat=True).distinct()
        periodic_task_name = PythonLogs.objects.all().values_list('periodic_task_name', flat=True).distinct()
        profile_yml_used_name  = PythonLogs.objects.all().values_list('profile_yml_used_name', flat=True).distinct()
        extra_context.update({
            "created_at": created_at,
            "completed_at": completed_at,
            "success": success,
            "repository_used_name": repository_used_name,
            "command": command,
            # "previous_command":previous_command,
            "periodic_task_name":periodic_task_name,
            "profile_yml_used_name":profile_yml_used_name

        })
        return super().changelist_view(request, extra_context=extra_context)


def python_logs_index(request):
    item_list= PythonLogs.objects.all()
    template= loader.get_template('admin/base.html')

    context={
        'itemList': item_list,
    }
    return HttpResponse(template.render(context, request))

@admin.register(ProfileYAML)
class ProfileYAMLAdmin(admin.ModelAdmin):
    list_display = [
        "name",
        "profile_yml",
        "colored_is_published",
       
    ]

    search_fields = ("name", "profile_yml")
    list_filter = ("name", "profile_yml",)

   
    
    change_form_template = 'admin/analytics/change_form.html'
    change_list_template = 'admin/analytics/change_list.html'

   

    # # def change_view(self, request, object_id, form_url="", extra_context=None):
    # #     extra_context = extra_context or {}
    # #     post = PythonLogs.objects.get(id=object_id)
    # #     extra_context["form"] = self.get_form(instance=post, request=request)
    # #     return super(PythonLogsLAdmin, self).change_view(request, object_id, form_url=form_url, extra_context=extra_context)

    # def add_view(self, request, form_url="", extra_context=None):
    #     change_form_template = 'admin/analytics/change_form.html'
    #     extra_context = extra_context or {}
    #     # extra_context['form'] = self.get_form(request)
    #     return super(ProfileYAMLAdmin, self).add_view(request, extra_context=extra_context)

    
    # def changelist_view(self, request, extra_context=None):
    #     change_list_template = 'admin/analytics/change_list.html'
    #     extra_context = {'title': 'Select Profile Yamls to Change'}
    #     extra_context = extra_context or {}
    #     name = ProfileYAML.objects.all().values_list('name', flat=True).distinct()
    #     profile_yml = ProfileYAML.objects.all().values_list('profile_yml', flat=True).distinct()
    #     extra_context.update({
    #         "name":name,
    #         "profile_yml":profile_yml,

    #     })
    #     return super().changelist_view(request, extra_context=extra_context)


    

    def has_add_permission(self, request):
        count = ProfileYAML.objects.all().count()
        if count < 2:
            return True
        return False

    def has_delete_permission(self, request, obj=None):
        return False


# class GitRepoVariantInline(admin.TabularInline):
#     model = GitRepo
#     readonly_fields = ('id',)
#     extra = 1
#     form = VariantForm    # new line

# @admin.display
# def colored_name(self):
#         return format_html(
#             '<span style="color: #{};">{} {}</span>',
#             self.name,
#             self.url,
#             self.color_code,
#         )

# @admin.display(description="Name")
# def upper_case_name(self, obj):
#         return f"{obj.id} {obj.name} {obj.public_key}".upper()


@admin.register(GitRepo)
class GitRepoAdmin(admin.ModelAdmin):
    # form = GitRepoForm

 

    list_display = [
       "id","name","ssh_key_id",
    ]


    # change_list_template = 'admin/base_site.html'
    search_fields = ("id", "name")
    list_filter = ('id','name')
    
    def add_view(self, request  , form_url="", extra_context=None):
        change_form_template = 'admin/analytics/change_form.html'
        extra_context = extra_context or {}
        # extra_context['form'] = self.get_form(request)
        return super(GitRepoAdmin, self).add_view(request, extra_context=extra_context)

    # # def change_view(self, request, object_id, form_url="", extra_context=None):
    # #     change_form_template = 'admin/analytics/change_form.html'
    # #     extra_context = extra_context or {}
    # #     post = GitRepo.objects.get(id=object_id)
    # #     extra_context["form"] = self.get_form(instance=post, request=request)
    # #     return super(GitRepoAdmin, self).change_view(request, object_id, form_url=form_url, extra_context=extra_context)

    def changelist_view(self, request, extra_context=None):
        change_list_template = 'admin/analytics/change_list.html'
        extra_context = {'title': 'Select GIT to Change'}
        extra_context = extra_context or {}
        id = GitRepo.objects.all().values_list('id', flat=True).distinct()
        name = GitRepo.objects.all().values_list('name', flat=True).distinct()
        # url = GitRepo.objects.all().values_list('url', flat=True).distinct()
        ssh_key_id  = GitRepo.objects.all().values_list('ssh_key_id', flat=True).distinct()
        extra_context.update({
            "id": id,
            "name": name,
            # "url": url,
            "ssh_key_id": ssh_key_id,
        })
        return super().changelist_view(request, extra_context=extra_context)

    # def changelist_view(self, request, extra_context=None):
    #    extra_context={
    #     'itemList': item_list,
    # }
    #    extra_context = extra_context or {}
    #    item_list= GitRepo.objects.all()
    #    extra_context.update({
    #        'itemList': item_list,
    #     })
    #    return super().changelist_view(request, extra_context=extra_context)


    def status_colored(self, obj):
         return mark_safe('<b style="background:{};">{}</b>'.format('red', 'Foo'))
      
    # def _kw(self, obj):
    #   colors = {
    #     'YES': 'green',
    #     'PERAPHS': 'yellow',
    #     'NO': 'red',
    # }
    #   return format_html(
    #     '<span style="background-color:{};">{}</span>',
    #     colors[obj.status],
    #     obj.status,
    # )
    
    # # list_display = [
    # #    "id",upper_case_name,"public_key",
    # ]
    # inlines = [GitRepoVariantInline]

    def save_model(self, request, obj, form, change):
        obj.save()
        result, msg = clone_git_repo(obj)
        if result:
            ...
        else:
            obj.delete()
            messages.error(request, f"Something is wrong while git cloning {msg}")

    def index():
        item_list= GitRepo.objects.all()
        template= loader.get_template('analytics/base_site.html')
        context={
        'itemList': item_list,
        }
        return HttpResponse(template.render(context, request)) 
    
    

                  

@admin.register(SubProcessLog)
class SubprocessAdmin(admin.ModelAdmin):
    list_display = [
        "created_at",
        "details",
    ]




class ProfileSelectWidget(Select):
    """Widget that lets you choose between task names."""

    celery_app = current_app
    _choices = None

    def profiles_as_choices(self):
        _ = self._modules  # noqa
        tasks = list(
            sorted(
                name for name in self.celery_app.tasks if not name.startswith("celery.")
            )
        )
        return (("", ""),) + tuple(zip(tasks, tasks))

    @property
    def choices(self):
        if self._choices is None:
            self._choices = self.profiles_as_choices()
        return self._choices

    @choices.setter
    def choices(self, _):
        pass

    @cached_property
    def _modules(self):
        self.celery_app.loader.import_default_modules()


class ProfileChoiceField(forms.ChoiceField):
    widget = ProfileSelectWidget

    def valid_value(self, value):
        return True


class PeriodicTaskForm(BasePeriodicTaskForm):
    profile_yml = ProfileChoiceField(
        label="Profile YAML",
        required=False,
    )

    class Meta:
        model = PeriodicTask
        exclude = ()

class PeriodicTaskAdmin(BasePeriodicTaskAdmin):
    # form = PeriodicTaskForm
    model = PeriodicTask
    list_display = ('__str__', 'id',  'enabled', 'interval', 'start_time',
                    'last_run_at', 'one_off')
    fieldsets = (
        (
            None,
            {
                "fields": (
                    "name",
                    "git_repo",
                    "profile_yml",
                    "regtask",
                    "task",
                    "enabled",
                    "description",
                ),
                "classes": ("extrapretty", "wide"),
            },
        ),
        (
            "Schedule",
            {
                "fields": (
                    "interval",
                    "crontab",
                    "solar",
                    "clocked",
                    "start_time",
                    "last_run_at",
                    "one_off",
                ),
                "classes": ("extrapretty", "wide"),
            },
        ),
        (
            "Arguments",
            {
                "fields": ("args",),
                "classes": ("extrapretty", "wide", "collapse", "in"),
            },
        ),
        (
            "Execution Options",
            {
                "fields": (
                    "expires",
                    "expire_seconds",
                    "queue",
                    "exchange",
                    "routing_key",
                    "priority",
                    "headers",
                ),
                "classes": ("extrapretty", "wide", "collapse", "in"),
            },
        ),
    )



   

if PeriodicTask in admin.site._registry:
    admin.site.unregister(PeriodicTask)
admin.site.register(PeriodicTask, PeriodicTaskAdmin)


# admin.site.unregister(Group)
# admin.site.register(Site)

admin.site.register(User)


