class oppo(Exception):
    def __init__(self, message="A custom error occurred"):
        super().__init__(message)


def noOppositeFound(self,key):
    existing_opposite=self.get_opposite()
    if len(existing_opposite) == 0:
        return """
        [bold Magenta]Error:[/]
No opposite relationships set:
Run: 
`register_opposite(your_weaviate_client, opposite_refs)`

where opposite_refs has format:

`
opposite_refs="CollectionName.referenceName<->CollectionName.referenceName"
`
for example:
`
opposite_refs=Employee.hasDepartment<->Department.hasEmployee
`

"""
    else:
        s =f"""[bold Magenta]Error:[/] No opposite found for {key}"""
    return s