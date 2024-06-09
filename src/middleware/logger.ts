function logger(req: any, res: any, next: any) {
    console.log("Logging...");
    next();
};

export default logger;