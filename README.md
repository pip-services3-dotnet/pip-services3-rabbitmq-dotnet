# <img src="https://uploads-ssl.webflow.com/5ea5d3315186cf5ec60c3ee4/5edf1c94ce4c859f2b188094_logo.svg" alt="Pip.Services Logo" width="200"> <br/> RabbitMQ specific components for .NET

This library is a part of [Pip.Services](https://github.com/pip-services/pip-services) project.
The RabbitMQ module contains a set of components for working with the message queue in RabbitMQ through the AMQP protocol.

The module contains the following packages:
- **Build** - Factory for constructing module components
- **Queue** - Message Queuing components that implement the standard [Messaging](https://github.com/pip-services3-dotnet/pip-services3-messaging-dotnet) module interface

<a name="links"></a> Quick links:

* [Configuration](https://www.pipservices.org/recipies/configuration)
* [API Reference](https://pip-services3-dotnet.github.io/pip-services3-rabbitmq-dotnet/)
* [Change Log](CHANGELOG.md)
* [Get Help](https://www.pipservices.org/community/help)
* [Contribute](https://www.pipservices.org/community/contribute)


## Use

Install the dotnet package as
```bash
dotnet add package PipServices3.Rabbitmq
```

## Develop

For development you shall install the following prerequisites:
* Core .NET SDK 3.1+
* Visual Studio Code or another IDE of your choice
* Docker

Restore dependencies:
```bash
dotnet restore src/src.csproj
```

Compile the code:
```bash
dotnet build src/src.csproj
```

Run automated tests:
```bash
dotnet restore test/test.csproj
dotnet test test/test.csproj
```

Generate API documentation:
```bash
./docgen.ps1
```

Before committing changes run dockerized build and test as:
```bash
./build.ps1
./test.ps1
./clear.ps1
```

## Contacts

The .NET version of Pip.Services is created and maintained by:
- **Sergey Seroukhov**

Many thanks to contibutors, who put their time and talant into making this project better:
- **Andrew Harrington**, Kyrio
