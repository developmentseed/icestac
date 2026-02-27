import aws_cdk as cdk

from config import Config
from icestac_stack import IcestacStack

config = Config()

app = cdk.App()
IcestacStack(app, config.stack_name("stack"), config=config)
app.synth()
