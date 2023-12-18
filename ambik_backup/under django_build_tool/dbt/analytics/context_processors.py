from dbt.analytics.models import (
    GitRepo,
    ProfileYAML,
    SSHKey,
    PeriodicTask as PeriodicTaskModel,
)
def dashboard(request):
    return {'model1': GitRepo.objects.all() }